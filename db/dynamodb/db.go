package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

type dynamodbWrapper struct {
	client              *dynamodb.Client
	tablename           *string
	primaryKeyType      string
	primarykey          string
	primarykeyPtr       *string
	hashKey             string
	hashKeyPtr          *string
	hashKeyValue        string
	readCapacityUnits   int64
	writeCapacityUnits  int64
	consistentRead      bool
	deleteAfterRun      bool
	command             string
	timeoutMilliseconds int64
	maxRetry            int
	getItemErrorFile    string
	putItemErrorFile    string
}

func (r *dynamodbWrapper) Close() error {
	var err error = nil
	if strings.Compare("run", r.command) == 0 {
		log.Printf("Ensuring that the table is deleted after the run stage...\n")
		if r.deleteAfterRun {
			err = r.deleteTable()
			if err != nil {
				log.Printf("Couldn't delete table after run. Here's why: %v\n", err)
			}
		}
	}
	return err
}

func (r *dynamodbWrapper) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (r *dynamodbWrapper) CleanupThread(_ context.Context) {
}

func (r *dynamodbWrapper) Read(ctx context.Context, table string, key string, fields []string) (data map[string][]byte, err error) {
	data = make(map[string][]byte, len(fields))

	// create a new context from the previous ctx with a timeout, e.g. 5 milliseconds
	ctx, cancel := context.WithTimeout(ctx, time.Duration(r.timeoutMilliseconds)*time.Millisecond)
	defer cancel()

	response, err := r.client.GetItem(ctx, &dynamodb.GetItemInput{
		Key:            r.GetKey(key),
		TableName:      r.tablename,
		ConsistentRead: aws.Bool(r.consistentRead),
	})

	if err != nil {
		// log.Printf("Couldn't get info about %v. Here's why: %v\n", key, err)
		file, ferr := os.OpenFile(r.getItemErrorFile, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
		if ferr != nil {
			log.Fatal(ferr)
		}
		defer file.Close()
		logger := log.New(file, "", log.LstdFlags)
		logger.Printf("Couldn't get info about %q. Here's why: %v\n", key, err)
	} else {
		err = attributevalue.UnmarshalMap(response.Item, &data)
		if err != nil {
			log.Printf("Couldn't unmarshal response. Here's why: %v\n", err)
		}
	}
	delete(data, r.primarykey)
	// log.Printf("new data: %q\n", data)
	return

}

// GetKey returns the composite primary key of the document in a format that can be
// sent to DynamoDB.
func (r *dynamodbWrapper) GetKey(key string) map[string]types.AttributeValue {
	if r.primaryKeyType == "HASH_AND_RANGE" {
		return map[string]types.AttributeValue{
			// If the primary key type is HASH_AND_RANGE, then what has been put
			// into the attributes map above is the range key part of the primary
			// key, we still need to put in the hash key part here.
			r.hashKey:    &types.AttributeValueMemberB{Value: []byte(r.hashKeyValue)},
			r.primarykey: &types.AttributeValueMemberB{Value: []byte(key)},
		}
	} else {
		return map[string]types.AttributeValue{
			r.primarykey: &types.AttributeValueMemberB{Value: []byte(key)},
		}
	}
}

func (r *dynamodbWrapper) Scan(ctx context.Context, table string, startKey string, count int, fields []string) (result []map[string][]byte, err error) {
	limit := int32(count)
	data := make([]map[string][]byte, len(fields))

	response, err := r.client.Scan(context.TODO(), &dynamodb.ScanInput{
		TableName:         r.tablename,
		ExclusiveStartKey: r.GetKey(startKey),
		AttributesToGet:   fields,
		Limit:             &limit,
	})

	if err != nil {
		panic(fmt.Sprintf("failed to Scan items, %v", err))
	}

	err = attributevalue.UnmarshalListOfMaps(response.Items, &data)
	if err != nil {
		panic(fmt.Sprintf("failed to unmarshal Dynamodb Scan Items, %v", err))
	}

	result = append(result, data...)

	return
}

func (r *dynamodbWrapper) Update(ctx context.Context, table string, key string, values map[string][]byte) (err error) {
	var upd = expression.UpdateBuilder{}
	for name, value := range values {
		upd = upd.Set(expression.Name(name), expression.Value(&types.AttributeValueMemberB{Value: value}))
	}
	expr, err := expression.NewBuilder().WithUpdate(upd).Build()

	// create a new context from the previous ctx with a timeout, e.g. 5 milliseconds
	ctx, cancel := context.WithTimeout(ctx, time.Duration(r.timeoutMilliseconds)*time.Millisecond)
	defer cancel()

	_, err = r.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		Key:                       r.GetKey(key),
		TableName:                 r.tablename,
		UpdateExpression:          expr.Update(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		ReturnValues:              "ALL_OLD",
	})
	if err != nil {
		log.Printf("Couldn't update item to table. Here's why: %v\nUpdateExpression:%s\nExpressionAttributeNames:%s\n", err, *expr.Update(), expr.Names())
	}
	return
}

func (r *dynamodbWrapper) Insert(ctx context.Context, table string, key string, values map[string][]byte) (err error) {
	if r.primaryKeyType == "HASH_AND_RANGE" {
		values[r.hashKey] = []byte(r.hashKeyValue)
		values[r.primarykey] = []byte(key)
	} else {
		values[r.primarykey] = []byte(key)
	}

	item, err := attributevalue.MarshalMap(values)
	if err != nil {
		panic(err)
	}

	// create a new context from the previous ctx with a timeout, e.g. 1000 milliseconds
	ctx, cancel := context.WithTimeout(ctx, time.Duration(r.timeoutMilliseconds)*time.Millisecond)
	defer cancel()

	_, err = r.client.PutItem(ctx,
		&dynamodb.PutItemInput{
			TableName: r.tablename,
			Item:      item,
			// ConditionExpression: aws.String("attribute_not_exists(obj)"),
			ReturnValues: "ALL_OLD",
		})
	if err != nil {
		// log.Printf("Couldn't add %q to table. Here's why: %v\n", key, err)
		file, ferr := os.OpenFile(r.putItemErrorFile, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
		if ferr != nil {
			log.Fatal(ferr)
		}
		defer file.Close()
		logger := log.New(file, "", log.LstdFlags)
		logger.Printf("Couldn't add %q to table. Here's why: %v\n", key, err)
	}
	return
}

func (r *dynamodbWrapper) BatchInsert(ctx context.Context, table string, keys []string, values []map[string][]byte) (err error) {
	log.Printf("BatchInsert")

	input := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			table: make([]types.WriteRequest, len(values)),
		},
	}

	for i, value := range values {
		item, err := attributevalue.MarshalMap(value)
		if err != nil {
			panic(err)
		}
		input.RequestItems[table][i] = types.WriteRequest{
			PutRequest: &types.PutRequest{
				Item: item,
			},
		}
	}
	_, err = r.client.BatchWriteItem(ctx, input)

	if err != nil {
		log.Printf("Couldn't add items to table. Here's why: %v\n", err)
	}

	return
}

func (r *dynamodbWrapper) Delete(ctx context.Context, table string, key string) error {
	// create a new context from the previous ctx with a timeout, e.g. 5 milliseconds
	ctx, cancel := context.WithTimeout(ctx, time.Duration(r.timeoutMilliseconds)*time.Millisecond)
	defer cancel()

	_, err := r.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: r.tablename,
		Key:       r.GetKey(key),
	})
	return err
}

type dynamoDbCreator struct{}

// TableExists determines whether a DynamoDB table exists.
func (r *dynamodbWrapper) tableExists() (bool, error) {
	exists := true
	_, err := r.client.DescribeTable(
		context.TODO(), &dynamodb.DescribeTableInput{TableName: r.tablename},
	)
	if err != nil {
		var notFoundEx *types.ResourceNotFoundException
		if errors.As(err, &notFoundEx) {
			log.Printf("Table %v does not exist.\n", *r.tablename)
			err = nil
		} else {
			log.Printf("Couldn't determine existence of table %v. Here's why: %v\n", *r.tablename, err)
		}
		exists = false
	}
	return exists, err
}

// This function uses NewTableExistsWaiter to wait for the table to be created by
// DynamoDB before it returns.
func (r *dynamodbWrapper) createTable() (*types.TableDescription, error) {
	var tableDesc *types.TableDescription
	if r.primaryKeyType == "HASH_AND_RANGE" {
		table, err := r.client.CreateTable(context.TODO(), &dynamodb.CreateTableInput{
			AttributeDefinitions: []types.AttributeDefinition{
				{
					AttributeName: r.primarykeyPtr,
					AttributeType: types.ScalarAttributeTypeB,
				},
				{
					AttributeName: r.hashKeyPtr,
					AttributeType: types.ScalarAttributeTypeB,
				},
			},
			KeySchema: []types.KeySchemaElement{
				{
					AttributeName: r.primarykeyPtr,
					KeyType:       types.KeyTypeRange,
				},
				{
					AttributeName: r.hashKeyPtr,
					KeyType:       types.KeyTypeHash,
				},
			},
			TableName: r.tablename,
			ProvisionedThroughput: &types.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(r.readCapacityUnits),
				WriteCapacityUnits: aws.Int64(r.writeCapacityUnits),
			},
		})
		if err != nil {
			log.Printf("Couldn't create table %v. Here's why: %v\n", *r.tablename, err)
		} else {
			log.Printf("Waiting for table to be available.\n")
			waiter := dynamodb.NewTableExistsWaiter(r.client)
			err = waiter.Wait(context.TODO(), &dynamodb.DescribeTableInput{
				TableName: r.tablename}, 5*time.Minute)
			if err != nil {
				log.Printf("Wait for table exists failed. Here's why: %v\n", err)
			}
			tableDesc = table.TableDescription
		}
		return tableDesc, err
	} else {
		table, err := r.client.CreateTable(context.TODO(), &dynamodb.CreateTableInput{
			AttributeDefinitions: []types.AttributeDefinition{{
				AttributeName: r.primarykeyPtr,
				AttributeType: types.ScalarAttributeTypeB,
			}},
			KeySchema: []types.KeySchemaElement{
				{
					AttributeName: r.primarykeyPtr,
					KeyType:       types.KeyTypeHash,
				},
			},
			TableName: r.tablename,
			ProvisionedThroughput: &types.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(r.readCapacityUnits),
				WriteCapacityUnits: aws.Int64(r.writeCapacityUnits),
			},
		})
		if err != nil {
			log.Printf("Couldn't create table %v. Here's why: %v\n", *r.tablename, err)
		} else {
			log.Printf("Waiting for table to be available.\n")
			waiter := dynamodb.NewTableExistsWaiter(r.client)
			err = waiter.Wait(context.TODO(), &dynamodb.DescribeTableInput{
				TableName: r.tablename}, 5*time.Minute)
			if err != nil {
				log.Printf("Wait for table exists failed. Here's why: %v\n", err)
			}
			tableDesc = table.TableDescription
		}
		return tableDesc, err
	}
}

func (r dynamoDbCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	rds := &dynamodbWrapper{}

	rds.tablename = aws.String(p.GetString(tablename, tablenameDefault))
	// other than the primary key, you do not need to define
	// any extra attributes or data types when you create a table.
	rds.primaryKeyType = p.GetString(primaryKeyTypeFieldName, primaryKeyTypeFieldNameDefault)
	rds.primarykey = p.GetString(primaryKeyFieldName, primaryKeyFieldNameDefault)
	rds.primarykeyPtr = aws.String(rds.primarykey)
	rds.hashKey = p.GetString(hashKeyFieldName, hashKeyFieldNameDefault)
	rds.hashKeyPtr = aws.String(rds.hashKey)
	rds.hashKeyValue = p.GetString(hashKeyValue, hashKeyValueDefault)
	rds.timeoutMilliseconds = p.GetInt64(operationTimeoutFieldName, operationTimeoutFieldNameDefault)
	rds.maxRetry = p.GetInt(maxRetryFieldName, maxRetryFieldNameDefault)
	rds.getItemErrorFile = p.GetString(getItemErrorFileFieldName, getItemErrorFileFieldNameDefault)
	rds.putItemErrorFile = p.GetString(putItemErrorFileFieldName, putItemErrorFileFieldNameDefault)
	rds.readCapacityUnits = p.GetInt64(readCapacityUnitsFieldName, readCapacityUnitsFieldNameDefault)
	rds.writeCapacityUnits = p.GetInt64(writeCapacityUnitsFieldName, writeCapacityUnitsFieldNameDefault)
	rds.consistentRead = p.GetBool(consistentReadFieldName, consistentReadFieldNameDefault)
	rds.deleteAfterRun = p.GetBool(deleteTableAfterRunFieldName, deleteTableAfterRunFieldNameDefault)
	endpoint := p.GetString(endpointField, endpointFieldDefault)
	region := p.GetString(regionField, regionFieldDefault)
	rds.command, _ = p.Get(prop.Command)
	var err error = nil
	var cfg aws.Config
	if strings.Contains(endpoint, "localhost") && strings.Compare(region, "localhost") != 0 {
		log.Printf("given you're using dynamodb local endpoint you need to specify -p %s='localhost'. Ignoring %s and enforcing -p %s='localhost'\n", regionField, region, regionField)
		region = "localhost"
	}
	// fix: failed to get rate limit token, retry quota exceeded, 0 available, 5 requested
	tokenRateLimiter := config.WithRetryer(func() aws.Retryer {
		return retry.NewStandard(func(so *retry.StandardOptions) {
			so.RateLimiter = ratelimit.NewTokenRateLimit(10000000)
		})
	})
	// retry nums
	retryNums := config.WithRetryer(func() aws.Retryer {
		return retry.AddWithMaxAttempts(retry.NewStandard(), rds.maxRetry)
	})
	if strings.Compare(endpoint, endpointFieldDefault) == 0 {
		if strings.Compare(region, regionFieldDefault) != 0 {
			// if endpoint is default but we have region
			cfg, err = config.LoadDefaultConfig(context.TODO(), config.WithRegion(region), tokenRateLimiter, retryNums)
		} else {
			// if both endpoint and region are default
			cfg, err = config.LoadDefaultConfig(context.TODO(), tokenRateLimiter, retryNums)
		}
	} else {
		cfg, err = config.LoadDefaultConfig(context.TODO(),
			config.WithRegion(region),
			config.WithEndpointResolver(aws.EndpointResolverFunc(
				func(service, region string) (aws.Endpoint, error) {
					return aws.Endpoint{URL: endpoint, SigningRegion: region}, nil
				})),
			tokenRateLimiter,
			retryNums,
		)
	}
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}
	// Create DynamoDB client
	rds.client = dynamodb.NewFromConfig(cfg)
	exists, err := rds.tableExists()

	if strings.Compare("load", rds.command) == 0 {
		if !exists {
			_, err = rds.createTable()
		} else {
			ensureCleanTable := p.GetBool(ensureCleanTableFieldName, ensureCleanTableFieldNameDefault)
			if ensureCleanTable {
				log.Printf("dynamo table named %s already existed. Deleting it...\n", *rds.tablename)
				_ = rds.deleteTable()
				_, err = rds.createTable()
			} else {
				log.Printf("dynamo table named %s already existed. Skipping table creation.\n", *rds.tablename)
			}
		}
	} else {
		if !exists {
			log.Fatalf("dynamo table named %s does not exist. You need to run the load stage previous than '%s'...\n", *rds.tablename, "run")
		}
	}
	return rds, err
}

func (rds *dynamodbWrapper) deleteTable() error {
	_, err := rds.client.DeleteTable(context.TODO(), &dynamodb.DeleteTableInput{
		TableName: rds.tablename,
	})
	if err != nil {
		log.Fatalf("Unable to delete table, %v", err)
	}
	waiter := dynamodb.NewTableNotExistsWaiter(rds.client)
	err = waiter.Wait(context.TODO(), &dynamodb.DescribeTableInput{
		TableName: rds.tablename}, 5*time.Minute)
	if err != nil {
		log.Fatalf("Wait for table deletion failed. Here's why: %v", err)
	}
	return err
}

const (
	tablename        = "dynamodb.tablename"
	tablenameDefault = "ycsb"
	// The property "primaryKeyType" below specifies the type of primary key
	// you have setup for the test table. There are two choices:
	// - HASH (default)
	// - HASH_AND_RANGE
	primaryKeyTypeFieldName            = "dynamodb.primarykey.type"
	primaryKeyTypeFieldNameDefault     = "HASH"
	primaryKeyFieldName                = "dynamodb.primarykey"
	primaryKeyFieldNameDefault         = "_key"
	hashKeyFieldName                   = "dynamodb.hashkey"
	hashKeyFieldNameDefault            = "hashkey"
	hashKeyValue                       = "dynamodb.hashkey.value"
	hashKeyValueDefault                = "hash"
	operationTimeoutFieldName          = "dynamodb.request.timeout.ms"
	operationTimeoutFieldNameDefault   = 1000
	maxRetryFieldName                  = "dynamodb.maxretry"
	maxRetryFieldNameDefault           = 3
	getItemErrorFileFieldName          = "dynamodb.getitem.file"
	getItemErrorFileFieldNameDefault   = "getitem_error.log"
	putItemErrorFileFieldName          = "dynamodb.putitem.file"
	putItemErrorFileFieldNameDefault   = "putitem_error.log"
	readCapacityUnitsFieldName         = "dynamodb.rc.units"
	readCapacityUnitsFieldNameDefault  = 10
	writeCapacityUnitsFieldName        = "dynamodb.wc.units"
	writeCapacityUnitsFieldNameDefault = 10
	ensureCleanTableFieldName          = "dynamodb.ensure.clean.table"
	ensureCleanTableFieldNameDefault   = true
	endpointField                      = "dynamodb.endpoint"
	endpointFieldDefault               = ""
	regionField                        = "dynamodb.region"
	regionFieldDefault                 = ""
	// GetItem provides an eventually consistent read by default.
	// If your application requires a strongly consistent read, set ConsistentRead to true.
	// Although a strongly consistent read might take more time than an eventually consistent read, it always returns the last updated value.
	consistentReadFieldName             = "dynamodb.consistent.reads"
	consistentReadFieldNameDefault      = false
	deleteTableAfterRunFieldName        = "dynamodb.delete.after.run.stage"
	deleteTableAfterRunFieldNameDefault = false
)

func init() {
	ycsb.RegisterDBCreator("dynamodb", dynamoDbCreator{})
}
