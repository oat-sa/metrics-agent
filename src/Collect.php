<?php

namespace OAT\Agent;

use Aws\CloudWatch\CloudWatchClient;
use Aws\Credentials\Credentials;
use Aws\DynamoDb\DynamoDbClient;
use Hoa\Ruler\Context;
use Hoa\Ruler\Ruler;

class Collect
{
    /** @var \DateTimeImmutable  */
    protected $start;

    /** @var \DateTimeImmutable  */
    protected $end;

    /**
     * @var CloudWatchClient
     */
    protected $cloudWatchClient;

    protected $dynamodbClient;

    public function __construct(Credentials $credentials, $region)
    {
        $this->cloudWatchClient = new CloudWatchClient([
            'version' => 'latest',
            'region' => $region,
            'credentials' => $credentials,
        ]);

        $this->dynamodbClient = new DynamoDbClient([
            'version' => 'latest',
            'region' => $region,
            'credentials' => $credentials,
        ]);

        $this->start = new \DateTimeImmutable();
        $this->end = new \DateTimeImmutable();
    }

    public function run()
    {
        $ruleDown = 'isBiz AND consumedreadcapacityaverage(table, 2) < provisionedreadcapacity';
        $ruleUp = 'isBiz AND consumedreadcapacityaverage(table, 2) > provisionedreadcapacity';
        if ($this->rulesDynamoDbScaleDown($ruleDown)) {
            $this->scaleDownAction();
        }
        elseif ($this->rulesDynamoDbScaleUp($ruleUp)) {
            $this->scaleUpAction();
        }
    }

    protected function scaleDownAction()
    {
        echo "I want scale down!\n";
        $table = 'luoat22exx_deliveryExecution';
        var_dump($this->dynamodbClient->describeTable(["TableName"=>$table]));
        var_dump($this->dynamodbClient->describeLimits());
        var_dump($this->decreaseReadCapacity($table, 5, 5));
    }

    protected function scaleUpAction()
    {
        echo "I want scale up!\n";
        $table = 'luoat22exx_deliveryExecution';
        var_dump($this->dynamodbClient->describeTable(["TableName"=>$table]));
        var_dump($this->dynamodbClient->describeLimits());
        var_dump($this->decreaseReadCapacity($table, 10, 10));
    }

    protected function decreaseReadCapacity($table, $readCapacity, $writeCapacity)
    {
        $params = [
          'ProvisionedThroughput' => [
              'ReadCapacityUnits' => $readCapacity, // REQUIRED
              'WriteCapacityUnits' => $writeCapacity, // REQUIRED
          ],
          'TableName' => $table,
        ];
        return $this->dynamodbClient->updateTable($params);
    }
    protected function increaseReadCapacity($table, $readCapacity, $writeCapacity)
    {
        $params = [
          'ProvisionedThroughput' => [
              'ReadCapacityUnits' => $readCapacity, // REQUIRED
              'WriteCapacityUnits' => $writeCapacity, // REQUIRED
          ],
          'TableName' => $table,
        ];
        return $this->dynamodbClient->updateTable($params);
    }

    public function dynamodb($table, int $period)
    {
        $params = [
            'EndTime' => $this->end->format('c'),
            'MetricName' => 'ConsumedReadCapacityUnits',
            'Namespace' => 'AWS/DynamoDB',
            'Period' => 60,
            'StartTime' => $this->start->sub(new \DateInterval("PT{$period}H"))->format('c'),
            'Statistics' => ['SampleCount'],
            'Dimensions' => [[
                'Name' => 'TableName',
                'Value' => $table,
            ]],
            'Unit' => 'Count',
        ];

        $result = $this->cloudWatchClient->getMetricStatistics($params);

        if (empty($result['Datapoints'])) {
            return 0;
        }

        $averageReadCapacity = 0;

        foreach ($result['Datapoints'] as $datapoint) {
            $averageReadCapacity += $datapoint['SampleCount'];
        }

        //calc average and rebase in req per seconds
        $averageReadCapacity = ceil(($averageReadCapacity / count($result['Datapoints'])) / 60 * 2);

        return $averageReadCapacity;
    }

    protected function rulesDynamoDbScaleDown($ruleDown): bool
    {
        $ruler = new Ruler();
        $ruler->getDefaultAsserter()->setOperator('consumedreadcapacityaverage', [$this, 'dynamodb']);

        $table = 'luoat22exx_deliveryExecution';
        $describeDynamoDbTableResult = $this->dynamodbClient->describeTable(['TableName' => $table]);

        $context = new Context();
        $context['isBiz'] = true;
        $context['table'] = $table;
        $context['provisionedreadcapacity'] = $describeDynamoDbTableResult['Table']['ProvisionedThroughput']['ReadCapacityUnits'];
        $context['provisionedwritecapacity'] = $describeDynamoDbTableResult['Table']['ProvisionedThroughput']['WriteCapacityUnits'];

        return $ruler->assert($ruleDown, $context);
    }
    protected function rulesDynamoDbScaleUp($ruleUp): bool
    {
        $ruler = new Ruler();
        $ruler->getDefaultAsserter()->setOperator('consumedreadcapacityaverage', [$this, 'dynamodb']);

        $table = 'luoat22exx_deliveryExecution';
        $describeDynamoDbTableResult = $this->dynamodbClient->describeTable(['TableName' => $table]);

        $context = new Context();
        $context['isBiz'] = true;
        $context['table'] = $table;
        $context['provisionedreadcapacity'] = $describeDynamoDbTableResult['Table']['ProvisionedThroughput']['ReadCapacityUnits'];
        $context['provisionedwritecapacity'] = $describeDynamoDbTableResult['Table']['ProvisionedThroughput']['WriteCapacityUnits'];

        return $ruler->assert($ruleUp, $context);
    }
}
