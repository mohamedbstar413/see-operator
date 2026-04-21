package utils

import (
	"context"
	"fmt"

	seeoperatorv1 "github.com/mohamedbstar413/see-operator/api/v1"
	"github.com/mohamedbstar413/see-operator/internal/manifests"
	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
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
	res := []string{}
	for _, c := range pod.Spec.Containers {
		probe := c.LivenessProbe
		if probe != nil {
			switch {
			case probe.HTTPGet != nil:
				fmt.Printf("  HTTP GET: %s:%v%s\n",
					probe.HTTPGet.Host,
					probe.HTTPGet.Port,
					probe.HTTPGet.Path)
				url := ":" + string(probe.HTTPGet.Port.IntVal) + probe.HTTPGet.Path
				res = append(res, url)
			case probe.TCPSocket != nil:
				fmt.Printf("  TCP Socket: port %v\n", probe.TCPSocket.Port)

			case probe.Exec != nil:
				fmt.Printf("  Exec: %v\n", probe.Exec.Command)

			case probe.GRPC != nil:
				fmt.Printf("  GRPC: port %d\n", probe.GRPC.Port)
			}
		}
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
