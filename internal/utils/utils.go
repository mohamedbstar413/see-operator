package utils

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

func GetServicesInNamespace(ctx context.Context, c client.Client, namespace string) (*v1.ServiceList, error) {
	logger := log.FromContext(ctx)
	logger.Info(fmt.Sprintf("Listing all services in %s", namespace))
	serviceList := &v1.ServiceList{}
	if err := c.List(ctx, serviceList, client.InNamespace(namespace)); err != nil {
		logger.Error(err, "Failed to list services")
		return nil, err
	}
	return serviceList, nil
}

func GetServicePods(ctx context.Context, c client.Client, namespace string, svc *v1.Service) (*v1.PodList, error) {
	logger := log.FromContext(ctx)
	logger.Info(fmt.Sprintf("Getting pods for service %s in namespace %s", svc.Name, namespace))

	podList := &v1.PodList{}
	if err := c.List(ctx, podList, client.InNamespace(namespace), client.MatchingLabels(svc.Spec.Selector)); err != nil {
		logger.Error(err, "Failed to list pods")
		return nil, err
	}

	return podList, nil
}
