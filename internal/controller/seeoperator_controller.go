/*
Copyright 2026.

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

package controller

import (
	"context"
	"slices"

	manifests "github.com/mohamedbstar413/see-operator/internal/manifests"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	log "sigs.k8s.io/controller-runtime/pkg/log"

	seeoperatorv1 "github.com/mohamedbstar413/see-operator/api/v1"
	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	batchv1 "k8s.io/api/batch/v1"
)

// SeeOperatorReconciler reconciles a SeeOperator object
type SeeOperatorReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=see-operator.example.com,resources=seeoperators,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=see-operator.example.com,resources=seeoperators/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=see-operator.example.com,resources=seeoperators/finalizers,verbs=update

func (r *SeeOperatorReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// add seeoperator object to the scheme
	seeoperatorv1.AddToScheme(r.Scheme)
	// add probe object to scheme
	monitoringv1.AddToScheme(r.Scheme)

	// check for the operator existence in the cluster
	var seeOperatorLive seeoperatorv1.SeeOperator
	err := r.Get(ctx, req.NamespacedName, &seeOperatorLive)
	if err != nil && apierrors.IsNotFound(err) {
		logger.Error(err, "Failed to get SeeOperator, Probably deleted")
		//======TODO=======
		// clean all dependent resources (found inside status.probeNames) and remove finalizer if exist
		return ctrl.Result{}, nil
	}

	// check if the blackbox exporter is given in the CRD yaml or not
	blackboxExporterUrl := seeOperatorLive.Spec.BlackboxUrl
	if blackboxExporterUrl != "" {
		logger.Info("Blackbox exporter URL provided in the CRD", "URL", blackboxExporterUrl)
	} else {
		logger.Info("No Blackbox exporter URL provided in the CRD, using default URL")
		// create the blackbox exporter resources from templates
		appsv1.AddToScheme(r.Scheme)
		corev1.AddToScheme(r.Scheme)

		// read the configmap, service and deployment templates from the assets folder and create them in the cluster
		cm := manifests.GetBlackboxCM(r.Scheme)
		cm.Namespace = seeOperatorLive.Namespace

		//set owner reference to the CM as the  seeoperator CRD
		err = ctrl.SetControllerReference(&seeOperatorLive, cm, r.Scheme)
		if err != nil {
			return ctrl.Result{}, err
		}
		//check if the CM already exists, if not create it
		var existingCM corev1.ConfigMap
		err = r.Get(ctx, client.ObjectKey{Name: cm.Name, Namespace: cm.Namespace}, &existingCM)
		if err != nil && apierrors.IsNotFound(err) {
			logger.Info("Blackbox exporter ConfigMap not found, creating it")
			err = r.Create(ctx, cm)
			if err != nil {
				logger.Error(err, "Failed to create Blackbox exporter ConfigMap")
				return ctrl.Result{}, err
			}
			logger.Info("Created Blackbox exporter ConfigMap", "ConfigMap", cm.Name)
		} else if err != nil {
			logger.Error(err, "Failed to get Blackbox exporter ConfigMap")
			return ctrl.Result{}, err
		} else {
			logger.Info("Blackbox exporter ConfigMap already exists, skipping creation")
			return ctrl.Result{}, nil
		}

		bbService := manifests.GetBlackboxService(r.Scheme)
		bbService.Namespace = seeOperatorLive.Namespace

		//set owner reference to the service as the  seeoperator CRD
		err = ctrl.SetControllerReference(&seeOperatorLive, bbService, r.Scheme)
		if err != nil {
			return ctrl.Result{}, err
		}
		//check if the service already exists, if not create it
		var existingService corev1.Service
		err = r.Get(ctx, client.ObjectKey{Name: bbService.Name, Namespace: bbService.Namespace}, &existingService)
		if err != nil && apierrors.IsNotFound(err) {
			logger.Info("Blackbox exporter Service not found, creating it")
			err = r.Create(ctx, bbService)
			if err != nil {
				logger.Error(err, "Failed to create Blackbox exporter Service")
				return ctrl.Result{}, err
			}
			logger.Info("Created Blackbox exporter Service", "Service", bbService.Name)
		} else if err != nil {
			logger.Error(err, "Failed to get Blackbox exporter Service")
			return ctrl.Result{}, err
		} else {
			logger.Info("Blackbox exporter Service already exists, skipping creation")
			return ctrl.Result{}, nil
		}

		bbDeployment := manifests.GetBlackboxDeployment(r.Scheme)
		bbDeployment.Namespace = seeOperatorLive.Namespace

		//set owner reference to the deployment as the  seeoperator CRD

		//check if the deployment already exists, if not create it
		var existingDeployment appsv1.Deployment
		err = r.Get(ctx, client.ObjectKey{Name: bbDeployment.Name, Namespace: bbDeployment.Namespace}, &existingDeployment)
		if err != nil && apierrors.IsNotFound(err) {
			logger.Info("Blackbox exporter Deployment not found, creating it")
			err = ctrl.SetControllerReference(&seeOperatorLive, bbDeployment, r.Scheme)
			if err != nil {
				return ctrl.Result{}, err
			}
			err = r.Create(ctx, bbDeployment)
			if err != nil {
				logger.Error(err, "Failed to create Blackbox exporter Deployment")
				return ctrl.Result{}, err
			}
			logger.Info("Created Blackbox exporter Deployment", "Deployment", bbDeployment.Name)
			blackboxExporterUrl = bbService.Name + "." + bbService.Namespace + ".svc.cluster.local" + ":" + "9115"
		} else if err != nil {
			logger.Error(err, "Failed to get Blackbox exporter Deployment")
			return ctrl.Result{}, err
		} else {
			logger.Info("Blackbox exporter Deployment already exists, skipping creation")
			return ctrl.Result{}, nil
		}
	}

	//create the cronjob if not exists
	var cronjob *batchv1.CronJob
	cronjob = manifests.GetCronJobYaml(r.Scheme)
	cronjob.Namespace = seeOperatorLive.Namespace
	err = r.Get(ctx, client.ObjectKey{Name: cronjob.Name, Namespace: cronjob.Namespace}, cronjob)
	if err != nil && apierrors.IsNotFound(err) {
		logger.Info("CronJob not found, creating it")
		err = ctrl.SetControllerReference(&seeOperatorLive, cronjob, r.Scheme)
		if err != nil {
			logger.Error(err, "Failed to set controller reference for CronJob")
			return ctrl.Result{}, err
		}
		// CronJob does not exist, create it
		err = r.Create(ctx, cronjob)
		if err != nil {
			logger.Error(err, "Failed to create CronJob")
			return ctrl.Result{}, err
		}
		logger.Info("Created CronJob", "CronJob", cronjob.Name)
	} else if err != nil {
		logger.Error(err, "Failed to get CronJob")
		return ctrl.Result{}, err
	} else {
		logger.Info("CronJob already exists, skipping creation")
	}

	// get all namespaces to monitor from the CRD yaml
	namespacesToMonitor := seeOperatorLive.Spec.NamespacesToMonitor
	statusNamespacesToMonitor := seeOperatorLive.Status.Namespaces

	// 1) if both namespacesToMonitor and statusNamespacesToMonitor are nil, then do nothing
	if namespacesToMonitor == nil && statusNamespacesToMonitor == nil {
		logger.Info("Both namespacesToMonitor and statusNamespacesToMonitor are nil, doing nothing")
	}

	// 2) if namespacesToMonitor is nil and statusNamespacesToMonitor is not empty
	if namespacesToMonitor == nil && statusNamespacesToMonitor != nil {
		// remove all probes from the namespaces
		for _, ns := range statusNamespacesToMonitor {
			var probeList monitoringv1.ProbeList
			err = r.List(ctx, &probeList, client.InNamespace(ns))
			if err != nil && apierrors.IsNotFound(err) {
				logger.Error(err, "No Probes in namespace", "Namespace", ns)
				continue
			}
			for _, probe := range probeList.Items {
				err = r.Delete(ctx, &probe)
				if err != nil {
					logger.Error(err, "Failed to delete probe", "Probe", probe.Name)
					return ctrl.Result{}, err
				}
			}
			logger.Info("Deleted all probes in namespace", "Namespace", ns)
		}
		seeOperatorLive.Status.Namespaces = nil
		err = r.Status().Update(ctx, &seeOperatorLive)
		if err != nil {
			logger.Error(err, "Failed to update SeeOperator status")
			return ctrl.Result{}, err
		}
		logger.Info("Updated SeeOperator status to be empty")
	}

	// 3) if namespacesToMonitor is not nil and statusNamespacesToMonitor is empty
	if namespacesToMonitor != nil && statusNamespacesToMonitor == nil {
		for _, ns := range namespacesToMonitor {
			var podList corev1.PodList
			err = r.List(ctx, &podList, client.InNamespace(ns))
			if err != nil {
				logger.Error(err, "Failed to list pods in namespace", "Namespace", ns)
				return ctrl.Result{}, err
			}
			for _, pod := range podList.Items {
				for _, c := range pod.Spec.Containers {
					liveProbe := c.LivenessProbe
					if liveProbe == nil {
						logger.Info("Container has no liveness probe", "Container", c.Name)
						continue
					}
					switch {
					case liveProbe.HTTPGet != nil:
						http := liveProbe.HTTPGet
						logger.Info("Container has HTTP liveness probe", "Container", c.Name, "Path", http.Path, "Port", http.Port.String())
						probe := manifests.GetProbe(r.Scheme)
						probe.Name = pod.Name + "-" + c.Name + "-probe"
						probe.Namespace = ns
						probe.Spec.Targets.StaticConfig.Targets = []string{liveProbe.HTTPGet.Path}
						probe.Spec.ProberSpec.URL = blackboxExporterUrl

						prometheusLabels := seeOperatorLive.Spec.ProbeSelectorLabels
						if prometheusLabels == nil {
							logger.Info("No Prometheus labels provided in the CRD, using default labels")
							prometheusLabels = map[string]string{
								"createdBy": "see-operator",
							}
						} else {
							logger.Info("Using provided Prometheus labels from the CRD", "Labels", prometheusLabels)
						}
						probe.Labels = prometheusLabels

						err = r.Create(ctx, probe)
						if err != nil {
							logger.Error(err, "Failed to create probe for container", "Container", c.Name)
							return ctrl.Result{}, err
						}
						logger.Info("Created probe for container", "Container", c.Name, "Probe", probe.Name)

						seeOperatorLive.Status.ProbeNames = append(seeOperatorLive.Status.ProbeNames, probe.Name)
						if !slices.Contains(seeOperatorLive.Status.Namespaces, ns) {
							seeOperatorLive.Status.Namespaces = append(seeOperatorLive.Status.Namespaces, ns)
						}
						err = r.Status().Update(ctx, &seeOperatorLive)
						if err != nil {
							logger.Error(err, "Failed to update SeeOperator status")
							return ctrl.Result{}, err
						}
						logger.Info("Updated SeeOperator status with new probe and namespace", "Probe", probe.Name, "Namespace", ns)
					}
				}
			}
		}
	}

	// 4) both not nil --> if equal do nothing, if not equal --> make them equal
	if namespacesToMonitor != nil && statusNamespacesToMonitor != nil {
		slices.Sort(namespacesToMonitor)
		slices.Sort(statusNamespacesToMonitor)
		if slices.Equal(namespacesToMonitor, statusNamespacesToMonitor) {
			logger.Info("namespacesToMonitor and statusNamespacesToMonitor are equal, doing nothing")
		} else {
			logger.Info("namespacesToMonitor and statusNamespacesToMonitor are not equal, updating status to match the spec")
			if len(namespacesToMonitor) > len(statusNamespacesToMonitor) {
				logger.Info("More namespaces in spec than in status ==> we need to create probes for the new namespaces and update the status")
				for _, specNs := range namespacesToMonitor {
					if !slices.Contains(statusNamespacesToMonitor, specNs) {
						var nsPodList corev1.PodList
						err = r.List(ctx, &nsPodList, client.InNamespace(specNs))
						if err != nil {
							logger.Error(err, "Failed to list pods in namespace", "Namespace", specNs)
							return ctrl.Result{}, err
						}
						for _, pod := range nsPodList.Items {
							for _, c := range pod.Spec.Containers {
								liveProbe := c.LivenessProbe
								if liveProbe == nil {
									logger.Info("Container has no liveness probe")
									continue
								}
								switch {
								case liveProbe.HTTPGet != nil:
									http := liveProbe.HTTPGet
									logger.Info("Container has HTTP liveness probe", "Container", c.Name, "Path", http.Path, "Port", http.Port.String())
									probe := manifests.GetProbe(r.Scheme)
									probe.Name = pod.Name + "-" + c.Name + "-probe"
									probe.Namespace = specNs
									probe.Spec.Targets.StaticConfig.Targets = []string{liveProbe.HTTPGet.Path}
								}
							}
						}
					}
				}
			} else {
				logger.Info("More namespaces in status than in spec ==> we need to delete probes for the removed namespaces and update the status")
				for _, statusNs := range statusNamespacesToMonitor {
					if !slices.Contains(namespacesToMonitor, statusNs) {
						var probeList monitoringv1.ProbeList
						err = r.List(ctx, &probeList, client.InNamespace(statusNs))
						if err != nil && apierrors.IsNotFound(err) {
							logger.Error(err, "No Probes in namespace", "Namespace", statusNs)
							continue
						}
						for _, probe := range probeList.Items {
							err = r.Delete(ctx, &probe)
							if err != nil {
								logger.Error(err, "Failed to delete probe", "Probe", probe.Name)
								return ctrl.Result{}, err
							}
						}
						logger.Info("Deleted all probes in namespace", "Namespace", statusNs)
					}
				}
				seeOperatorLive.Status.Namespaces = namespacesToMonitor
				err = r.Status().Update(ctx, &seeOperatorLive)
				if err != nil {
					logger.Error(err, "Failed to update SeeOperator status")
					return ctrl.Result{}, err
				}
				logger.Info("Updated SeeOperator status to match the spec", "Namespaces", namespacesToMonitor)
			}
		}
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *SeeOperatorReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&seeoperatorv1.SeeOperator{}).
		Owns(&monitoringv1.Probe{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.Service{}).
		Owns(&batchv1.CronJob{}).
		Owns(&batchv1.Job{}).
		Complete(r)
}
