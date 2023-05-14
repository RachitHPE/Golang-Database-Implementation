package dynamodb

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// create local dynamoDB client.
func CreateLocalClient(port int) *dynamodb.Client {
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("us-west-2"),
		config.WithEndpointResolver(aws.EndpointResolverFunc(
			func(service, region string) (aws.Endpoint, error) {
				return aws.Endpoint{URL: fmt.Sprintf("http://localhost:%d", port)}, nil
			})),
		config.WithCredentialsProvider(credentials.StaticCredentialsProvider{
			Value: aws.Credentials{
				AccessKeyID: "dummy", SecretAccessKey: "dummy", SessionToken: "dummy",
				Source: "Hard-coded credentials; values are irrelevant for local DynamoDB",
			},
		}),
	)
	if err != nil {
		panic(err)
	}

	return dynamodb.NewFromConfig(cfg)
}

func createTable(client *dynamodb.Client) error {
	out, err := client.CreateTable(context.TODO(), &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("id"),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String("name"),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("id"),
				KeyType:       types.KeyTypeHash,
			},
			{
				AttributeName: aws.String("name"),
				KeyType:       types.KeyTypeRange,
			},
		},
		TableName:   aws.String("dummy-table"),
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		return err
	}

	fmt.Println(out)

	return nil

}

func listTables(client *dynamodb.Client) error {
	p := dynamodb.NewListTablesPaginator(client, nil, func(o *dynamodb.ListTablesPaginatorOptions) {
		o.StopOnDuplicateToken = true
	})

	for p.HasMorePages() {
		out, err := p.NextPage(context.TODO())
		if err != nil {
			panic(err)
		}

		for _, tn := range out.TableNames {
			fmt.Println(tn)
		}
	}

	return nil

}

func scanResultFromTable(client *dynamodb.Client) error {
	out, err := client.Scan(context.TODO(), &dynamodb.ScanInput{
		TableName: aws.String("dummy-table"),
	})
	if err != nil {
		panic(err)
	}

	fmt.Println(out.Items)

	return nil

}

func scanResultWithFilter(client *dynamodb.Client) error {
	out, err := client.Scan(context.TODO(), &dynamodb.ScanInput{
		TableName:        aws.String("dummy-table"),
		FilterExpression: aws.String("attribute_not_exists(deletedAt) AND contains(name, :name)"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":name": &types.AttributeValueMemberS{Value: "Rachit"},
		},
	})
	if err != nil {
		panic(err)
	}

	fmt.Println(out.Items)

	return nil

}

func getResultFromTable(client *dynamodb.Client) error {
	out, err := client.GetItem(context.TODO(), &dynamodb.GetItemInput{
		TableName: aws.String("dummy-table"),
		Key: map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{Value: "123"},
		},
	})

	if err != nil {
		panic(err)
	}

	fmt.Println(out.Item)

	return nil

}

func getUsingStructMarshal(client *dynamodb.Client) error {
	key := struct {
		ID string `dynamodbav:"id" json:"id"`
	}{ID: "123"}
	avs, err := attributevalue.MarshalMap(key)
	if err != nil {
		panic(err)
	}

	out, err := client.GetItem(context.TODO(), &dynamodb.GetItemInput{
		TableName: aws.String("dummy-table"),
		Key:       avs,
	})
	if err != nil {
		panic(err)
	}

	fmt.Println(out.Item)

	return nil

}

func insertIntoTable(client *dynamodb.Client) error {
	out, err := client.PutItem(context.TODO(), &dynamodb.PutItemInput{
		TableName: aws.String("dummy-table"),
		Item: map[string]types.AttributeValue{
			"id":   &types.AttributeValueMemberS{Value: "123"},
			"name": &types.AttributeValueMemberS{Value: "Rachit"},
		},
	})

	if err != nil {
		panic(err)
	}

	fmt.Println(out.Attributes)

	return nil

}

func DynamodbInitialization() {
	client := CreateLocalClient(8003)

	err := createTable(client)
	if err != nil {
		fmt.Println("error in creating table ", err)
	}

	err = insertIntoTable(client)
	if err != nil {
		fmt.Println("error is ", err)
	}

	err = getResultFromTable(client)
	if err != nil {
		fmt.Println("error is ", err)
	}

	err = listTables(client)
	if err != nil {
		fmt.Println("error is ", err)
	}

	err = getUsingStructMarshal(client)
	if err != nil {
		fmt.Println("error is ", err)
	}

}
