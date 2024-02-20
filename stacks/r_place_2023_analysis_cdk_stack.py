import aws_cdk as cdk
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_glue as glue
import aws_cdk.aws_iam as iam
import aws_cdk.aws_s3_deployment as s3deploy

class RPlace2023AnalysisCdkStack(cdk.Stack):

    def __init__(self, scope: cdk.App, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
 

        glue_role = iam.Role(
            self, 'glue_role_id2323',
            assumed_by=iam.ServicePrincipal('glue.amazonaws.com'),
            managed_policies=[iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSGlueServiceRole')]
        )

        glueS3Bucket = s3.Bucket(self, 'reddit-data-bucket', 
        versioned= True,
        block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
        removal_policy=cdk.RemovalPolicy.DESTROY,
        auto_delete_objects=True
        )

        glueS3Bucket.grant_read_write(glue_role)

        # Upload glue script to the specified bucket
        s3deploy.BucketDeployment(self, 'deployment',
                                  sources=[s3deploy.Source.asset('./assets/')],
                                  destination_bucket=glueS3Bucket,
                                  destination_key_prefix='assets/')
        

        # setup AWS glue job
        glue_workflow = glue.CfnWorkflow(self, 'r_place_etl_workflow',
                                                name='r_place_etl_workflow',
                                                description='Workflow to process the ETL for r/place.')


        download_r_place_job = glue.CfnJob(self, 'download_r_place_csvs_job',
                        name='download_r_place_csvs_job',
                        command=glue.CfnJob.JobCommandProperty(
                            name='pythonshell',
                            python_version='3.9',
                            script_location=f's3://{glueS3Bucket.bucket_name}/assets/download_r_place_csvs.py'),
                        role=glue_role.role_arn,
                        glue_version='3.0',
                        max_capacity=1,
                        timeout=3, 
                        default_arguments={
                            '--bucket': glueS3Bucket.bucket_name
                        })
        
        process_csvs_to_parquet_job = glue.CfnJob(self, 'process_csvs_to_parquet_job',
                name='process_csvs_to_parquet_job',
                command=glue.CfnJob.JobCommandProperty(
                    name='glueetl',
                    python_version='3',
                    script_location=f's3://{glueS3Bucket.bucket_name}/assets/process_csvs_to_parquet.py'),
                role=glue_role.role_arn,
                glue_version='4.0',
                worker_type='G.1X',
                number_of_workers=10,
                timeout=60, 
                default_arguments={
                    '--bucket': glueS3Bucket.bucket_name
                })
        
 
        # Triggers configuration
        download_trigger = \
         glue.CfnTrigger(self, 'start_trigger',
            name='start_trigger',
            actions=[glue.CfnTrigger.ActionProperty(job_name=download_r_place_job.name)],
            type='ON_DEMAND',
            workflow_name=glue_workflow.name)
        
        download_finished_trigger = \
            glue.CfnTrigger(self, 'download_finished_trigger',
                name='download_finished_trigger',
                actions=[glue.CfnTrigger.ActionProperty(job_name=process_csvs_to_parquet_job.name)],
                type='CONDITIONAL',
                start_on_creation=True,
                workflow_name=glue_workflow.name,
                predicate=glue.CfnTrigger.PredicateProperty(
                    conditions=[glue.CfnTrigger.ConditionProperty(
                        state='SUCCEEDED',
                        logical_operator='EQUALS',
                        job_name=download_r_place_job.name)]))
        
        glue_database = glue.CfnDatabase(self, 'r_place_db',
                                        catalog_id=cdk.Aws.ACCOUNT_ID,
                                        database_input=glue.CfnDatabase.DatabaseInputProperty(
                                            name='r_place_db',
                                            description='Database for rplace data.'))
    
        glue_crawler = glue.CfnCrawler(self, 'r_place_crawler',
            role=glue_role.role_arn,
            database_name='r_place_db',
            targets={'s3Targets': [{'path': f's3://{glueS3Bucket.bucket_name}/processed/'}]},
            name='r_place_crawler')

        parquet_finished_trigger = \
            glue.CfnTrigger(self, 'parquet_finished_trigger',
                name='parquet_finished_trigger',
                actions=[glue.CfnTrigger.ActionProperty(crawler_name=glue_crawler.name)],
                type='CONDITIONAL',
                start_on_creation=True,
                workflow_name=glue_workflow.name,
                predicate=glue.CfnTrigger.PredicateProperty(
                    conditions=[glue.CfnTrigger.ConditionProperty(
                        state='SUCCEEDED',
                        logical_operator='EQUALS',
                        job_name=process_csvs_to_parquet_job.name)]))
