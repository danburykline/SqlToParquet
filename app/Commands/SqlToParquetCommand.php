<?php

namespace App\Commands;

use codename\parquet\data\DataColumn;
use codename\parquet\data\DataField;
use codename\parquet\data\Schema;
use codename\parquet\ParquetWriter;
use Illuminate\Console\Scheduling\Schedule;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Storage;
use LaravelZero\Framework\Commands\Command;
use function Termwind\{render};

class SqlToParquetCommand extends Command
{
    /**
     * The signature of the command.
     *
     * @var string
     */
    protected $signature = 'parquet';

    /**
     * The description of the command.
     *
     * @var string
     */
    protected $description = 'Reads data from Database formats it into parquet and uploads it to s3.';

    /**
     * Execute the console command.
     *
     * @return mixed
     */
    public function handle()
    {
        $tables = DB::select('SHOW TABLES');
        $db = 'Tables_in_'.DB::getDatabaseName();
        foreach ($tables as $table) {
            $table = $table->{$db};

            $columns = DB::select('SHOW COLUMNS FROM ' . $table);

            $schema = [];
            $groups = [];

            foreach ($columns as $column) {
                $data = DB::table($table)->pluck($column->Field)->toArray();

                $field = $column->Field;

                $type = $this->sqlFormatToPhpFormat($column->Type);

                $dataColumn = new DataColumn(DataField::createFromType($field, $type), $data);

                array_push($schema, $dataColumn->getField());
                array_push($groups, $dataColumn);
            }

            $schema = new Schema($schema);

            $fileName = DB::getDatabaseName() . '_' . $table . '.parquet';
            $fileStream = fopen(__DIR__.'/'. $fileName, 'w+');

            $parquetWriter = new ParquetWriter($schema, $fileStream);
            $groupWriter = $parquetWriter->CreateRowGroup();

            foreach ($groups as $group) {
                $groupWriter->WriteColumn($group);
            }

            $groupWriter->finish();
            $parquetWriter->finish();

            $this->info('Created parquet file for table: ' . $table);

            // uploads file to S3.
            Storage::disk('s3')-> put($fileName, __DIR__.'/'. $fileName);
            $this->info('Filename: ' . $table  .' uploaded to s3');

            $this->deleteFile($fileName, $table);

            $this->newLine();
        }
        return;
    }

    /**
     * Define the command's schedule.
     *
     * @param  \Illuminate\Console\Scheduling\Schedule  $schedule
     * @return void
     */
    public function schedule(Schedule $schedule)
    {
        // $schedule->command(static::class)->everyMinute();
    }

    /**
     * Deletes File
     *
     * @param $filename
     * @param $table
     *
     * @return void
     */
    private function deleteFile($filename, $table)
    {
        if (!unlink(__DIR__.'/'. $filename)) {
            $this->error('File for table ' . $table . ' cannot be deleted due to an error');
        }
        else {
            $this->info('File for table ' . $table . ' has been deleted');
        }
    }

    /**
     * Return SQL data type in acceptable PHP format
     *
     * @param $type
     * @return string
     */
    private function sqlFormatToPhpFormat($type)
    {
        if (str_contains($type, 'enum') || str_contains($type, 'varchar')) {
            return 'string';
        } else {
            return match ($type) {
                'int' => 'integer',
                'int unsigned' => 'integer',
                'double' => 'double',
                'tinyint' => 'long',
                'tinyint(1)' => 'long',
                'smallint' => 'integer',
                'mediumint' => 'integer',
                'bigint' => 'long',
                'blob' => 'string',
                'decimal(11,2)' => 'decimal',
                'timestamp' => 'string',
                'text' => 'string',
                'date' => 'string',
                'time' => 'string',
                'datetime' => 'string',
                'float' => 'float',
            };
        }
    }
}
