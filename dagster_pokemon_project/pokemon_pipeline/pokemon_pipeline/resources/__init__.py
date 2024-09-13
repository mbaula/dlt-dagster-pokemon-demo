from dagster import ConfigurableResource
import dlt

class DltPipeline(ConfigurableResource):
    pipeline_name: str
    dataset_name: str
    destination: str

    def create_pipeline(self, resource_data, table_name):
        pipeline = dlt.pipeline(
            pipeline_name=self.pipeline_name, 
            destination=self.destination, 
            dataset_name=self.dataset_name
        )

        load_info = pipeline.run(resource_data, table_name=table_name)

        return load_info
