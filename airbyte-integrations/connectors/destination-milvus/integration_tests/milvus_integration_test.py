#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import json
import logging
import os

from destination_milvus.destination import DestinationMilvus
from langchain.embeddings import OpenAIEmbeddings
from langchain.vectorstores import Milvus
from pymilvus import Collection, CollectionSchema, DataType, FieldSchema, connections, utility

from airbyte_cdk.destinations.vector_db_based.embedder import OPEN_AI_VECTOR_SIZE
from airbyte_cdk.destinations.vector_db_based.test_utils import BaseIntegrationTest
from airbyte_cdk.models import DestinationSyncMode, Status


class MilvusIntegrationTest(BaseIntegrationTest):
    """
    Zilliz call to create the collection: /v1/vector/collections/create
    {
        "collectionName": "test2",
        "dimension": 1536,
        "metricType": "L2",
        "vectorField": "vector",
        "primaryField": "pk"
    }
    """

    def _init_milvus(self):
        """Initialize Milvus Lite connection and clean up any existing test collections."""
        import tempfile
        import os
        
        # Create a temporary directory for Milvus Lite database
        self.temp_dir = tempfile.mkdtemp(prefix="milvus_lite_")
        db_path = os.path.join(self.temp_dir, "milvus.db")
        
        # Connect using Milvus Lite
        connections.connect(
            alias="test_driver",
            uri=f"sqlite:{db_path}",
            use_lite=True
        )
        
        # Clean up any existing test collections
        if utility.has_collection(self.config["indexing"]["collection"], using="test_driver"):
            utility.drop_collection(self.config["indexing"]["collection"], using="test_driver")

    def setUp(self):
        """Set up test environment with Milvus Lite and test configuration."""
        # Try to load from secrets first, fall back to template if not available
        config_path = "secrets/config.json" if os.path.exists("secrets/config.json") else "config_template.json"
        with open(config_path, "r") as f:
            self.config = json.loads(f.read())
        
        # Override host configuration to use Milvus Lite
        self.config["indexing"]["host"] = f"sqlite://{self.db_path}"
        self.config["indexing"]["auth"] = {"mode": "no_auth"}
        
        self._init_milvus()
        
    def tearDown(self):
        """Clean up Milvus Lite resources."""
        import shutil
        
        try:
            # Disconnect from Milvus Lite
            connections.disconnect("test_driver")
            
            # Remove temporary directory
            if hasattr(self, 'temp_dir'):
                shutil.rmtree(self.temp_dir, ignore_errors=True)
        except Exception as e:
            logging.warning(f"Error during teardown: {str(e)}")
            # Continue with cleanup even if there are errors

    def test_check_valid_config(self):
        outcome = DestinationMilvus().check(logging.getLogger("airbyte"), self.config)
        assert outcome.status == Status.SUCCEEDED

    def _create_collection(self, vector_dimensions=1536):
        """Create a test collection with specified vector dimensions using Milvus Lite."""
        pk = FieldSchema(name="pk", dtype=DataType.INT64, is_primary=True, auto_id=True)
        vector = FieldSchema(name="vector", dtype=DataType.FLOAT_VECTOR, dim=vector_dimensions)
        schema = CollectionSchema(fields=[pk, vector], enable_dynamic_field=True)
        collection = Collection(name=self.config["indexing"]["collection"], schema=schema, using="test_driver")
        # Note: Milvus Lite only supports FLAT index type
        collection.create_index(
            field_name="vector",
            index_params={"metric_type": "L2", "index_type": "FLAT"}
        )

    def test_check_valid_config_pre_created_collection(self):
        self._create_collection()
        outcome = DestinationMilvus().check(logging.getLogger("airbyte"), self.config)
        assert outcome.status == Status.SUCCEEDED

    def test_check_invalid_config_vector_dimension(self):
        self._create_collection(vector_dimensions=666)
        outcome = DestinationMilvus().check(logging.getLogger("airbyte"), self.config)
        assert outcome.status == Status.FAILED

    def test_check_invalid_config(self):
        outcome = DestinationMilvus().check(
            logging.getLogger("airbyte"),
            {
                "processing": {"text_fields": ["str_col"], "metadata_fields": [], "chunk_size": 1000},
                "embedding": {"mode": "openai", "openai_key": "mykey"},
                "indexing": {
                    "host": "https://notmilvus.com",
                    "collection": "test2",
                    "auth": {
                        "mode": "token",
                        "token": "mytoken",
                    },
                    "vector_field": "vector",
                    "text_field": "text",
                },
            },
        )
        assert outcome.status == Status.FAILED

    def test_write(self):
        self._init_milvus()
        catalog = self._get_configured_catalog(DestinationSyncMode.overwrite)
        first_state_message = self._state({"state": "1"})
        first_record_chunk = [self._record("mystream", f"Dogs are number {i}", i) for i in range(5)]

        # initial sync
        destination = DestinationMilvus()
        list(destination.write(self.config, catalog, [*first_record_chunk, first_state_message]))
        collection = Collection(self.config["indexing"]["collection"], using="test_driver")
        collection.flush()
        assert len(collection.query(expr="pk != 0")) == 5

        # incrementalally update a doc
        incremental_catalog = self._get_configured_catalog(DestinationSyncMode.append_dedup)
        list(destination.write(self.config, incremental_catalog, [self._record("mystream", "Cats are nice", 2), first_state_message]))
        collection.flush()
        result = collection.search(
            anns_field=self.config["indexing"]["vector_field"],
            param={},
            data=[[0] * OPEN_AI_VECTOR_SIZE],
            limit=10,
            expr='_ab_record_id == "mystream_2"',
            output_fields=["text"],
        )
        assert len(result[0]) == 1
        assert result[0][0].entity.get("text") == "str_col: Cats are nice"

        # test langchain integration
        embeddings = OpenAIEmbeddings(openai_api_key=self.config["embedding"]["openai_key"])
        vs = Milvus(
            embedding_function=embeddings,
            collection_name=self.config["indexing"]["collection"],
            connection_args={
                "uri": self.config["indexing"]["host"],
                "use_lite": True
            },
        )
        vs.fields.append("text")
        vs.fields.append("_ab_record_id")
        # call  vs.fields.append() for all fields you need in the metadata

        result = vs.similarity_search("feline animals", 1)
        assert result[0].metadata["_ab_record_id"] == "mystream_2"
