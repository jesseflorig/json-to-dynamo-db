# JSON to DynamoDB

A utility project to quickly convert any JSON to batch write to [DynamoDB](https://aws.amazon.com/dynamodb/).

## Prerequesits

You will need to have an AWS account and the [AWS CLI](https://aws.amazon.com/cli/) tools installed and configured

For now, you also have to create the tables via the AWS Console :( .

## Workflow

To easily load your JSON data into DynamoDB, follow these steps:

- Add any `.json` files to `/src`
- Run `node index.js`

*Note: The data will be pushed to tables matching the file name*

## Todo

- [ ] Create the tables via SDK
- [ ] Convert the batch-write to SDK
- [ ] Cleanup logic flow and make asynchronous
- [ ] Clean up conversion logic (recusrive maps)
- [ ] Maybe create a UI
- [ ] Linting / Tests
- [ ] Clear `/out` directory when done
