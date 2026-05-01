package utils

import (
	"context"
	"fmt"
	"strconv"

	seeoperatorv1 "github.com/mohamedbstar413/see-operator/api/v1"
	"github.com/mohamedbstar413/see-operator/internal/manifests"
	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
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

func GetLivenessProbesOfPod(ctx context.Context, pod v1.Pod) ([]string, error) {
	var res []string

	for _, c := range pod.Spec.Containers {
		probe := c.LivenessProbe
		if probe == nil || probe.HTTPGet == nil {
			continue
		}

		p := probe.HTTPGet

		// port handling (supports int + named ports)
		var port string
		if p.Port.Type == intstr.Int {
			port = strconv.Itoa(int(p.Port.IntVal))
		} else {
			port = p.Port.StrVal
		}

		// normalize path
		path := p.Path
		if path == "" {
			path = "/"
		}

		// return ONLY fragment (keep your current design)
		res = append(res, ":"+port+path)
	}

	return res, nil
}

func CreateProbe(ctx context.Context, c client.Client, seeOperator *seeoperatorv1.SeeOperator, scheme runtime.Scheme, probeName string, probeTargetName string, blackboxExporterUrl string, ns string, promethLabels map[string]string) (*monitoringv1.Probe, error) {
	logger := log.FromContext(ctx)
	probe := manifests.GetProbe(&scheme)
	probe.Name = probeName
	probe.Namespace = ns
	probe.Spec.Targets.StaticConfig.Targets = []string{probeTargetName}
	probe.Spec.ProberSpec.URL = blackboxExporterUrl
	probe.Labels = promethLabels
	probe.Spec.Module = "http_2xx"
	probe.Spec.JobName = probeName

	err := ctrl.SetControllerReference(seeOperator, probe, &scheme)
	if err != nil {
		logger.Error(err, "Failed to set controller reference for CronJob", "cronjob")
		return nil, err
	}
	err = c.Create(ctx, probe)
	if err != nil && apierrors.IsAlreadyExists(err) {
		logger.Info("probe ", probeName, " already exists")
		return probe, nil
	} else if err != nil {
		logger.Error(err, "Failed Create", " probe ", probeName)
		return nil, err
	}
	logger.Info("Created Probe ", "probe name", probeName)
	return probe, nil
}
