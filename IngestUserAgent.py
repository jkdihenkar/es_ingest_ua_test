# Author : JayD ( jkdihenkar@gmail.com )
# Version : v0.1

import sys
import logging
import json
import elasticsearch


class IngestUserAgent():

    def __init__(self):
        self.connect_to_es()
        self.load_ingest_pipeline('detect_browser', 'ingest_pipeline_ua.json')

    def connect_to_es(self):
        """
        Connect to ES
        """
        self.es = elasticsearch.Elasticsearch(
            'https://127.0.0.1:8084',
            http_auth=('admin', 'changeme'),
            verify_certs=False
        )
        logging.info("Connected to es : {}".format(self.es.info()))

    def load_ingest_pipeline(self, pipeline_name, pipeline_json_file):
        """
        Load Ingest attachment
        :param pipeline_name: 
        :param pipeline_json_file: 
        :return: None
        """
        with open(pipeline_json_file) as ingest_json_file:
            pipeline_details = json.load(ingest_json_file)

        try:
            print("The pipeline {} exists in es as {}".format(
                pipeline_name,
                self.es.ingest.get_pipeline(pipeline_name))
            )
        except elasticsearch.exceptions.NotFoundError as notfounderr:
            print("Searching for {} failed : {}".format(pipeline_name, notfounderr))
            resp = self.es.ingest.put_pipeline(
                pipeline_name,
                pipeline_details
            )
            print("Result of pipeline put for {} is {}".format(
                pipeline_name,
                resp
            ))

    def load_test_data(self, dataset_file):
        with open(dataset_file) as inputfile:
            for line in inputfile:
                line_s = line.strip()
                parsed_items = line_s.split('\t', -1)
                print(self.load_to_es(parsed_items))


    def load_to_es(self, list):
        """
        list[0] = uuid
        list[1] = useragent
        """
        try:
            if 'time' not in list[0].strip().lower():
                return self.es.index(
                    index='test_ingest_pipeline',
                    doc_type='test',
                    body={
                        'uuid': list[0].strip(),
                        'browser_details': list[1].strip()
                    },
                    pipeline='detect_browser'
                )
            else:
                return None
        except:
            return None


if __name__ == '__main__':
    script = IngestUserAgent()
    script.connect_to_es()
    script.load_ingest_pipeline('detect_browser', 'ingest_pipeline_ua.json')
    script.load_test_data('sample_dataset')
