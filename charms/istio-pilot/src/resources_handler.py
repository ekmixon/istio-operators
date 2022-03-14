#! /usr/bin/env python3
# Copyright 2021 Canonical Ltd.
# See LICENSE file for licensing details.

"""Resources handling library using Lightkube."""

import glob
from typing import Any, Type

import yaml
from jinja2 import Environment, FileSystemLoader
from lightkube import Client, codecs
from lightkube.core.exceptions import ApiError
from lightkube.generic_resource import create_namespaced_resource, GenericNamespacedResource
from lightkube.resources.core_v1 import Service


class ResourceHandler:
    def __init__(self, config):
        """A Lightkube API interface.

        Args:
            config:
                - "app_name": name of the application
                - "model_name": name of the Juju model this charm is deployed to
        """

        self.app_name = config["app_name"]
        self.model_name = config["model_name"]

        # Every lightkube API call will use the model name as the namespace by default
        self.lightkube_client = Client(namespace=self.model_name, field_manager="lightkube")

        self.env = Environment(loader=FileSystemLoader('src'))

    def delete_object(
        self, obj, namespace=None, ignore_not_found=False, ignore_unauthorized=False
    ):
        try:
            self.lightkube_client.delete(type(obj), obj.metadata.name, namespace=namespace)
        except ApiError as err:
            self.log.exception("ApiError encountered while attempting to delete resource.")
            if err.status.message is not None:
                if "not found" in err.status.message and ignore_not_found:
                    self.log.error(f"Ignoring not found error:\n{err.status.message}")
                elif "(Unauthorized)" in err.status.message and ignore_unauthorized:
                    # Ignore error from https://bugs.launchpad.net/juju/+bug/1941655
                    self.log.error(f"Ignoring unauthorized error:\n{err.status.message}")
                else:
                    self.log.error(err.status.message)
                    raise
            else:
                raise

    def delete_existing_resource_objects(
        self,
        resource,
        namespace=None,
        ignore_not_found=False,
        ignore_unauthorized=False,
        labels={},
    ):
        for obj in self.lightkube_client.list(
            resource,
            labels={"app.juju.is/created-by": f"{self.app_name}"}.update(labels),
            namespace=namespace,
        ):
            self.delete_object(
                obj,
                namespace=namespace,
                ignore_not_found=ignore_not_found,
                ignore_unauthorized=ignore_unauthorized,
            )

    def apply_manifest(self, manifest, namespace=None):
        for obj in codecs.load_all_yaml(manifest):
            self.lightkube_client.apply(obj, namespace=namespace)

    def delete_manifest(
        self, manifest, namespace=None, ignore_not_found=False, ignore_unauthorized=False
    ):
        for obj in codecs.load_all_yaml(manifest):
            self.delete_object(
                obj,
                namespace=namespace,
                ignore_not_found=ignore_not_found,
                ignore_unauthorized=ignore_unauthorized,
            )

    def reconcile_desired_resources(
        self, kind: Any, desired_resources: str, namespace: str = None
    ) -> None:
        """Reconciles the desired list of resources of any kind.

        Args:
            kind: resource kind (e.g. Service, Pod)
            desired_resource: all desired resources in manifest form as str
            namespace (optional): namespace of the object
        """

        existing_resources = self.lightkube_client.list(
            res=kind,
            labels={
                "app.juju.is/created-by": f"{self.app_name}",
                f"app.{self.app_name}.io/is-workload-entity": "true",
            },
            namespace=namespace,
        )

        if desired_resources:
            desired_resources = codecs.load_all_yaml(desired_resources)
            diff_obj = set(existing_resources) - set(desired_resources)
            for obj in diff_obj:
                self.delete_object(obj)
            self._apply_manifest(desired_resources, namespace=namespace)
        else:
            self.delete_existing_resource_objects(
                resource=kind,
                labels={
                    f"app.{self.app_name}.io/is-workload-entity": "true",
                },
                namespace=namespace,
            )

    def create_ns_resource(
        self, filename: str, context: dict = None
    ) -> Type[GenericNamespacedResource]:
        """Returns a class representing a namespaced K8s resource.

        Args:
            - filename: name of the manifest file
            - context: a map with key-value pairs for rendering manifests
        """
        t = self.env.get_template(filename)
        manifest = yaml.safe_load(t.render(context))
        ns_resource = create_namespaced_resource(
            group=manifest["apiVersion"].split("/")[0],
            version=manifest["apiVersion"].split("/")[1],
            kind=manifest["kind"],
            plural=f"{manifest['kind']}s".lower(),
            verbs=None,
        )

        return ns_resource
