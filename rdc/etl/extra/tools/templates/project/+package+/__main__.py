from rdc.etl.status.console import ConsoleStatus
from .example_job import ExampleJob

def main():
    for JobFactory in (ExampleJob, ):
        job = JobFactory()
        job.status.append(ConsoleStatus())

        print 'Running', job
        job()

if __name__ == '__main__':
    main()