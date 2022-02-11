/*
SPDX-License-Identifier: Apache-2.0

Copyright Contributors to the Submariner project.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.
*/

package controllers

import (
	"fmt"

	"github.com/pkg/errors"
	"github.com/submariner-io/admiral/pkg/federate"
	"github.com/submariner-io/admiral/pkg/stringset"
	"github.com/submariner-io/admiral/pkg/syncer"
	"github.com/submariner-io/admiral/pkg/util"
	submarinerv1 "github.com/submariner-io/submariner/pkg/apis/submariner.io/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog"
)

func startIngressEndpointsController(svc *corev1.Service, config *syncer.ResourceSyncerConfig) (*ingressEndpointsController, error) {
	var err error

	_, gvr, err := util.ToUnstructuredResource(&submarinerv1.GlobalIngressIP{}, config.RestMapper)
	if err != nil {
		return nil, errors.Wrap(err, "error converting resource")
	}

	controller := &ingressEndpointsController{
		baseSyncerController: newBaseSyncerController(),
		svcName:              svc.Name,
		namespace:            svc.Namespace,
		ingressIPMap:         stringset.NewSynchronized(),
	}

	fieldSelector := fields.Set(map[string]string{"metadata.name": svc.Name}).AsSelector().String()

	controller.resourceSyncer, err = syncer.NewResourceSyncer(&syncer.ResourceSyncerConfig{
		Name:                "Ingress Endpoints syncer",
		ResourceType:        &corev1.Endpoints{},
		SourceClient:        config.SourceClient,
		SourceNamespace:     svc.Namespace,
		RestMapper:          config.RestMapper,
		Federator:           federate.NewCreateFederator(config.SourceClient, config.RestMapper, corev1.NamespaceAll),
		Scheme:              config.Scheme,
		Transform:           controller.process,
		SourceFieldSelector: fieldSelector,
		ResourcesEquivalent: areEndpointsEqual,
	})

	if err != nil {
		return nil, errors.Wrap(err, "error creating the syncer")
	}

	if err := controller.Start(); err != nil {
		return nil, errors.Wrap(err, "error creating the syncer")
	}

	ingressIPs := config.SourceClient.Resource(*gvr).Namespace(corev1.NamespaceAll)
	controller.reconcile(ingressIPs, "" /* labelSelector */, fieldSelector, func(obj *unstructured.Unstructured) runtime.Object {
		endpointsName, exists, _ := unstructured.NestedString(obj.Object, "spec", "serviceRef", "name")
		if exists {
			return &corev1.Endpoints{
				ObjectMeta: metav1.ObjectMeta{
					Name:      endpointsName,
					Namespace: obj.GetNamespace(),
				},
			}
		}

		return nil
	})

	klog.Infof("Created Endpoints controller for (%s/%s) with selector %q", svc.Namespace, svc.Name, fieldSelector)

	return controller, nil
}

func (c *ingressEndpointsController) process(from runtime.Object, numRequeues int, op syncer.Operation) (runtime.Object, bool) {
	endpoints := from.(*corev1.Endpoints)
	key, _ := cache.MetaNamespaceKeyFunc(endpoints)

	ingressIP := &submarinerv1.GlobalIngressIP{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("ep-%.60s", endpoints.Name),
			Namespace: endpoints.Namespace,
			Labels: map[string]string{
				ServiceRefLabel: c.svcName,
			},
		},
	}

	if op == syncer.Delete {
		c.ingressIPMap.Remove(ingressIP.Name)
		klog.Infof("Ingress Endpoints %s for service %s deleted", key, c.svcName)

		return ingressIP, false
	}

	if c.ingressIPMap.Contains(ingressIP.Name) {
		// Avoid assigning ingressIPs to endpoints that are not ready with an endpoint IP
		return nil, false
	}

	klog.Infof("%q ingress Endpoints %s for service %s", op, key, c.svcName)

	// TODO: consider handling multiple endpoints IPs
	if len(endpoints.Subsets) == 0 || len(endpoints.Subsets[0].Addresses) == 0 {
		return nil, false
	}

	ingressIP.ObjectMeta.Annotations = map[string]string{
		headlessSvcEndpointsIP: endpoints.Subsets[0].Addresses[0].IP,
	}

	ingressIP.Spec = submarinerv1.GlobalIngressIPSpec{
		Target:     submarinerv1.HeadlessServiceEndpoints,
		ServiceRef: &corev1.LocalObjectReference{Name: c.svcName},
	}

	c.ingressIPMap.Add(ingressIP.Name)

	return ingressIP, false
}
