import json
import logging
from asyncio import sleep
from pathlib import Path

import aiohttp
import pytest
from bs4 import BeautifulSoup as BS
from juju.tag import untag
from pytest_operator.plugin import OpsTest

log = logging.getLogger(__name__)


@pytest.mark.abort_on_fail
async def test_deploy_bundle(ops_test: OpsTest):
    await ops_test.deploy_bundle(serial=True, extra_args=['--trust'])
    await ops_test.model.wait_for_idle(timeout=60 * 10)

    await ops_test.run(
        'kubectl',
        'wait',
        '--for=condition=available',
        'deployment',
        '--all',
        '--all-namespaces',
        '--timeout=5m',
        check=True,
    )
    for attempt in range(60):
        try:
            await ops_test.run(
                'kubectl',
                'get',
                'crd',
                'gateways.networking.istio.io',
                check=True,
            )
        except Exception:
            await sleep(1)
        else:
            break
    else:
        pytest.fail("Timed out waiting for Gateway CRD")

    root_url = 'https://raw.githubusercontent.com/istio/istio/release-1.11/samples/bookinfo'
    await ops_test.run(
        'kubectl',
        'label',
        'namespace',
        'default',
        'istio-injection=enabled',
        '--overwrite=true',
        check=True,
    )

    await ops_test.run(
        'kubectl',
        'apply',
        '-f',
        f'{root_url}/platform/kube/bookinfo.yaml',
        check=True,
    )
    for attempt in range(2):
        try:
            await ops_test.run(
                'kubectl',
                'wait',
                '--for=condition=ready',
                'pod',
                '--all',
                '-n=default',
                '--timeout=5m',
                check=True,
            )
        except AssertionError:
            if attempt > 0:
                raise
            # This means the command failed; there's a race condition of no
            # pods existing yet so just sleep a moment and try again.
            # See: https://github.com/kubernetes/kubernetes/issues/83242
            await sleep(2)
        else:
            break

    # Wait to create the VirtualService until we know the pods are ready,
    # otherwise there's a race condition where Istio can cache the "not ready"
    # state and requests will always fail with 503 (service unavailable) even
    # after the pods do come up.
    await ops_test.run(
        'kubectl',
        'apply',
        '-f',
        f'{root_url}/networking/bookinfo-gateway.yaml',
        check=True,
    )

    gateway_addr = await get_gateway_addr(ops_test)

    async with aiohttp.ClientSession(raise_for_status=True) as client:
        results = await client.get(f'http://{gateway_addr}/productpage')
        soup = BS(await results.text())

    assert soup.title.string == 'Simple Bookstore App'


async def test_ingress(ops_test: OpsTest, client_model):
    base_path = Path(__file__).parent.parent
    ingress_lib_path = base_path / "charms/istio-pilot/lib/charms/istio_pilot/v0/ingress.py"
    ingress_charm_path = base_path / "tests/data/ingress-test"
    ingress_charm_path = ops_test.render_charm(
        ingress_charm_path,
        context={"ingress_lib": ingress_lib_path.read_text()},
    )
    ingress_charm = await ops_test.build_charm(ingress_charm_path)

    ingress_app = await client_model.deploy(
        ingress_charm,
        num_units=3,
        resources={"httpbin-image": "kennethreitz/httpbin"},
    )
    await client_model.block_until(lambda: len(ingress_app.units) == 3, timeout=10 * 60)
    # would be nice if wait_for_idle accepted multiple statuses
    await client_model.block_until(
        lambda: all(
            unit.workload_status in {"blocked", "active", "error"} for unit in ingress_app.units
        ),
        timeout=10 * 60,
    )
    await client_model.wait_for_idle(raise_on_blocked=False)

    # finding the leader should not be this difficult
    status = await client_model.get_status()
    units_status = status.applications["ingress-test"]["units"]
    ingress_leader = None
    for ingress_unit in ingress_app.units:
        if units_status[ingress_unit.name].get("leader", False):
            assert ingress_unit.workload_status == "blocked"
            assert ingress_unit.workload_status_message == "Missing relation: ingress"
            ingress_leader = ingress_unit
        else:
            assert ingress_unit.workload_status == "active"

    assert ingress_leader is not None

    gateway_addr = await get_gateway_addr(ops_test)

    offer, saas, relation = None, None, None
    try:
        offer = await ops_test.model.create_offer("istio-pilot:ingress")
        model_owner = untag("user-", ops_test.model.info.owner_tag)
        saas = await client_model.consume(f"{model_owner}/{ops_test.model_name}.istio-pilot")
        relation = await ingress_app.add_relation("ingress", "istio-pilot:ingress")
        await client_model.wait_for_idle(status="active", timeout=60)
        action = await ingress_leader.run_action("get-urls")
        output = await action.wait()
        assert output.status == "completed", dict(output)
        action_result = output.results
        assert action_result["url"] == f"http://{gateway_addr}/ingress-test/"
        async with aiohttp.ClientSession(raise_for_status=True) as client:
            response = await client.get(action_result["url"] + "uuid")
            page_text = await response.text()
            assert "uuid" in page_text
            unit_urls = json.loads(action_result["unit-urls"])
            for unit in ingress_app.units:
                assert unit.name in unit_urls
            for unit_name, unit_url in unit_urls.items():
                unit_num = unit_name.split("/")[-1]
                assert unit_url == f"http://{gateway_addr}/ingress-test-unit-{unit_num}/"
                response = await client.get(unit_url + "uuid")
                page_text = await response.text()
                assert "uuid" in page_text
    finally:
        if not ops_test.keep_model:
            if relation:
                await ingress_app.remove_relation("ingress", "istio-pilot:ingress")
                await client_model.wait_for_idle(timeout=60)
                await ops_test.model.wait_for_idle(timeout=60)
            if saas:
                await client_model.remove_saas("istio-pilot")
            if offer:
                await ops_test.model.remove_offer("istio-pilot")


async def get_gateway_addr(ops_test):
    if hasattr(ops_test, "gateway_addr"):
        return ops_test.gateway_addr
    for attempt in range(3):
        gateway_json = await ops_test.run(
            'kubectl',
            'get',
            'services/istio-ingressgateway',
            '-n',
            ops_test.model_name,
            '-ojson',
            check=True,
        )

        gateway_obj = json.loads(gateway_json[1])
        gateway_ingress = gateway_obj['status'].get('loadBalancer', {}).get('ingress')
        if gateway_ingress:
            gateway_ingress = gateway_ingress[0]
            # cache for later usage
            ops_test.gateway_addr = gateway_ingress.get('host', gateway_ingress.get('ip'))
            return ops_test.gateway_addr
        await sleep(10)
    else:
        pytest.fail("Timed out waiting for gateway load-balancer address")
