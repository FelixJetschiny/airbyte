# Copyright (c) 2024 Airbyte, Inc., all rights reserved.

import json
from unittest import TestCase

from advetiser_slices import mock_advertisers_slices
from config_builder import ConfigBuilder
from source_tiktok_marketing import SourceTiktokMarketing

from airbyte_cdk.models import SyncMode
from airbyte_cdk.test.catalog_builder import CatalogBuilder
from airbyte_cdk.test.entrypoint_wrapper import read
from airbyte_cdk.test.mock_http import HttpMocker, HttpRequest, HttpResponse
from airbyte_cdk.test.mock_http.response_builder import find_template


class TestCreativeAssetsPortfolios(TestCase):
    stream_name = "creative_assets_portfolios"
    advertiser_id = "872746382648"

    def catalog(self, sync_mode: SyncMode = SyncMode.full_refresh):
        return CatalogBuilder().with_stream(name=self.stream_name, sync_mode=sync_mode).build()

    def config(self):
        return ConfigBuilder().build()

    @HttpMocker()
    def test_basic_read(self, http_mocker: HttpMocker):
        mock_advertisers_slices(http_mocker, self.config())

        http_mocker.get(
            HttpRequest(
                url="https://business-api.tiktok.com/open_api/v1.3/creative/portfolio/list/?page_size=100&advertiser_id=872746382648",
            ),
            HttpResponse(body=json.dumps(find_template(self.stream_name, __file__)), status_code=200),
        )

        output = read(SourceTiktokMarketing(config=self.config(), catalog=None, state=None), self.config(), self.catalog())
        assert len(output.records) == 2
