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
	"fmt"
	"slices"

	manifests "github.com/mohamedbstar413/see-operator/internal/manifests"
	"github.com/mohamedbstar413/see-operator/internal/utils"
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
	var sweep bool // to determine if there is a need to create new probes in already sweeped namespaces or not

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
	var blackboxExporterUrl string
	blackboxExporterUrlFromSpec := seeOperatorLive.Spec.BlackboxUrl
	blackboxExporterUrlFromStatus := seeOperatorLive.Status.BlackboxExporterUrl
	if blackboxExporterUrlFromSpec != "" || blackboxExporterUrlFromStatus != "" {
		if blackboxExporterUrlFromSpec != "" {
			blackboxExporterUrl = blackboxExporterUrlFromSpec
		} else {
			blackboxExporterUrl = blackboxExporterUrlFromStatus
		}
	}
	if blackboxExporterUrl != "" {
		logger.Info("Blackbox exporter URL provided in the CRD", "URL", blackboxExporterUrl)
		seeOperatorLive.Status.BlackboxExporterUrl = blackboxExporterUrl
		err = r.Status().Update(ctx, &seeOperatorLive)
		if err != nil {
			logger.Error(err, "Can't update blackbox exporter url in status", blackboxExporterUrl)
			return ctrl.Result{}, err
		}
	} else {
		logger.Info("No Blackbox exporter URL provided in the CRD, using default URL")
		// create the blackbox exporter resources from templates
		appsv1.AddToScheme(r.Scheme)
		corev1.AddToScheme(r.Scheme)

		// read the configmap, service and deployment templates from the assets folder and create them in the cluster
		cm := manifests.GetBlackboxCM(r.Scheme)
		cm.Namespace = seeOperatorLive.Namespace

		// set owner reference to the CM as the seeoperator CRD
		err = ctrl.SetControllerReference(&seeOperatorLive, cm, r.Scheme)
		if err != nil {
			return ctrl.Result{}, err
		}
		// check if the CM already exists, if not create it
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
		}

		bbService := manifests.GetBlackboxService(r.Scheme)
		bbService.Namespace = seeOperatorLive.Namespace

		// set owner reference to the service as the seeoperator CRD
		err = ctrl.SetControllerReference(&seeOperatorLive, bbService, r.Scheme)
		if err != nil {
			return ctrl.Result{}, err
		}
		// check if the service already exists, if not create it
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
		}

		bbDeployment := manifests.GetBlackboxDeployment(r.Scheme)
		bbDeployment.Namespace = seeOperatorLive.Namespace

		// check if the deployment already exists, if not create it
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
			//update seeoperator status
			seeOperatorLive.Status.BlackboxExporterUrl = blackboxExporterUrl
			err = r.Status().Update(ctx, &seeOperatorLive)
			if err != nil {
				logger.Error(err, "error updating blackboxexporterurl", blackboxExporterUrl)
				return ctrl.Result{}, err
			}
			logger.Info("Updated blackbox exporter url in status successfully!")
		} else if err != nil {
			logger.Error(err, "Failed to get Blackbox exporter Deployment")
			return ctrl.Result{}, err
		} else {
			logger.Info("Blackbox exporter Deployment already exists, skipping creation")
		}

		//now the blackbox all infra are created
		//update the status of operator
		blackboxExporterUrl = bbService.Name + "." + bbService.Namespace + ".svc.cluster.local" + ":9115"
		seeOperatorLive.Status.BlackboxExporterUrl = blackboxExporterUrl
		err = r.Status().Update(ctx, &seeOperatorLive)
		if err != nil {
			logger.Error(err, "Failed to get Blackbox exporter Deployment")
			return ctrl.Result{}, err
		}
	}

	// create the cronjob if not exists
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

	// get all pods in the namespacesToMonitor and see difference between them and the statusNamespacesToMonitor
	// to decide if we need to create new probes or delete existing probes or do nothing
	for _, nsToMonitor := range namespacesToMonitor {
		// List all endpoints in the namespace
		allEndpoints := &corev1.EndpointsList{}
		err = r.List(ctx, allEndpoints, client.InNamespace(nsToMonitor))
		if err != nil {
			logger.Error(err, "Failed to list endpoints", "Namespace", nsToMonitor)
			return ctrl.Result{}, err
		}

		probeList := &monitoringv1.ProbeList{}
		err = r.List(ctx, probeList, client.InNamespace(nsToMonitor))
		if err != nil {
			logger.Error(err, "Failed to list probes", "Namespace", nsToMonitor)
			return ctrl.Result{}, err
		}

		// Build a set of expected probe names from endpoints
		existingProbeNames := make(map[string]bool)
		for _, p := range probeList.Items {
			existingProbeNames[p.Name] = true
		}

		// Check if any endpoint doesn't have a corresponding probe yet
		for _, ep := range allEndpoints.Items {
			expectedProbeName := ep.Name + "-" + ep.Namespace + "-probe"
			if !existingProbeNames[expectedProbeName] {
				sweep = true
				break
			}
		}
	}

	// if new services are added to an ns that is already monitored ==> create a probe for that service
	if sweep {
		logger.Info("There is a need to sweep the namespaces to monitor to create new probes")
		for _, nsToMonitor := range namespacesToMonitor {
			logger.Info("Sweeping namespace to monitor", "Namespace", nsToMonitor)
			allEndpoints := &corev1.EndpointsList{}
			err = r.List(ctx, allEndpoints, client.InNamespace(nsToMonitor))
			if err != nil {
				logger.Error(err, "Failed to list endpoints in namespace", "Namespace", nsToMonitor)
				return ctrl.Result{}, err
			}
			// FIX: iterate endpoints.Items (one Endpoints object per service)
			for _, endpoints := range allEndpoints.Items {
				var relatedPods []corev1.Pod
				// FIX: iterate endpoints.Subsets (not endpoints.Items)
				for _, subset := range endpoints.Subsets {
					allAddresses := append(subset.Addresses, subset.NotReadyAddresses...)
					for _, addr := range allAddresses {
						if addr.TargetRef == nil || addr.TargetRef.Kind != "Pod" {
							continue // skip non-pod endpoints (e.g. external IPs)
						}
						pod := &corev1.Pod{}
						// FIX: use r.Get instead of c.Get
						err = r.Get(ctx, client.ObjectKey{
							Name:      addr.TargetRef.Name,
							Namespace: addr.TargetRef.Namespace,
						}, pod)
						if err != nil {
							// FIX: return ctrl.Result{} instead of nil
							return ctrl.Result{}, fmt.Errorf("failed to get pod %s/%s: %w",
								addr.TargetRef.Namespace, addr.TargetRef.Name, err)
						}
						relatedPods = append(relatedPods, *pod)
					}
				}
				// FIX: > 0 instead of > =
				if len(relatedPods) > 0 {
					firstPod := relatedPods[0]
					// FIX: check err == nil (success case), not err != nil
					allLivenessUrls, err := utils.GetLivenessProbesOfPod(ctx, firstPod)
					if err == nil && len(allLivenessUrls) > 0 {
						// FIX: declare variables properly
						probeName := endpoints.Name + "-" + endpoints.Namespace + "-probe"
						probeTargetName := endpoints.Name + "." + endpoints.Namespace + ".svc.cluster.local" + allLivenessUrls[0]
						logger.Info("Probe taregt is ", "probeTargetName ", probeTargetName)
						if slices.Contains(seeOperatorLive.Status.ProbeNames, probeName) {
							continue
						}
						createdProbe, err := utils.CreateProbe(ctx, r.Client, &seeOperatorLive, *r.Scheme, probeName, probeTargetName, blackboxExporterUrl, endpoints.Namespace, seeOperatorLive.Spec.ProbeSelectorLabels)
						if err != nil {
							logger.Error(err, "Failed Create Probe Reconcile", "probeName", probeName)
							return ctrl.Result{}, err
						}
						seeOperatorLive.Status.ProbeNames = append(seeOperatorLive.Status.ProbeNames, probeName)
						err = r.Status().Update(ctx, &seeOperatorLive)
						if err != nil {
							logger.Error(err, "Failed Update probeNames in operator status", "probeName", probeName)
							errRemove := r.Delete(ctx, createdProbe)
							if errRemove != nil {
								logger.Error(errRemove, "Failed delete probe", "probeName", probeName)
								return ctrl.Result{}, errRemove
							}
							return ctrl.Result{}, err
						}
					}
				}
			}
		}
	} else {
		logger.Info("No need to sweep")
	}

	// now cases for matching number of namespacesToMonitor in status and statusNamespacesToMonitor

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
		seeOperatorLive.Status.ProbeNames = nil
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
			allEndpoints := &corev1.EndpointsList{}
			// FIX: use allEndpoints, remove trailing client.
			err = r.List(ctx, allEndpoints, client.InNamespace(ns))
			if err != nil && apierrors.IsNotFound(err) {
				logger.Info("No Endpoints in namespace", "Namespace", ns)
				continue
			} else if err != nil {
				logger.Error(err, "Can't get endpoints in namespace", "Namespace", ns)
				return ctrl.Result{}, err
			}
			// FIX: iterate endpoints.Items (one Endpoints per service)
			for _, endpoints := range allEndpoints.Items {
				var relatedPods []corev1.Pod
				// FIX: iterate endpoints.Subsets (not endpoints.Items)
				for _, subset := range endpoints.Subsets {
					allAddresses := append(subset.Addresses, subset.NotReadyAddresses...)
					for _, addr := range allAddresses {
						if addr.TargetRef == nil || addr.TargetRef.Kind != "Pod" {
							continue
						}
						pod := &corev1.Pod{}
						// FIX: use r.Get instead of c.Get
						err = r.Get(ctx, client.ObjectKey{
							Name:      addr.TargetRef.Name,
							Namespace: addr.TargetRef.Namespace,
						}, pod)
						if err != nil {
							// FIX: return ctrl.Result{} instead of nil
							return ctrl.Result{}, fmt.Errorf("failed to get pod %s/%s: %w",
								addr.TargetRef.Namespace, addr.TargetRef.Name, err)
						}
						relatedPods = append(relatedPods, *pod)
					}
				}
				// FIX: > 0 instead of > =
				if len(relatedPods) > 0 {
					firstPod := relatedPods[0]
					// FIX: check err == nil (success case), not err != nil
					allLivenessUrls, err := utils.GetLivenessProbesOfPod(ctx, firstPod)
					if err == nil && len(allLivenessUrls) > 0 {
						// FIX: declare variables properly
						probeName := endpoints.Name + "-" + endpoints.Namespace + "-probe"
						probeTargetName := endpoints.Name + "." + endpoints.Namespace + ".svc.cluster.local" + allLivenessUrls[0]
						createdProbe, err := utils.CreateProbe(ctx, r.Client, &seeOperatorLive, *r.Scheme, probeName, probeTargetName, blackboxExporterUrl, endpoints.Namespace, seeOperatorLive.Spec.ProbeSelectorLabels)
						if err != nil {
							logger.Error(err, "Failed Create Probe Reconcile", "probeName", probeName)
							return ctrl.Result{}, err
						}
						seeOperatorLive.Status.ProbeNames = append(seeOperatorLive.Status.ProbeNames, probeName)
						err = r.Status().Update(ctx, &seeOperatorLive)
						if err != nil {
							logger.Error(err, "Failed Update probeNames in operator status", "probeName", probeName)
							errRemove := r.Delete(ctx, createdProbe)
							if errRemove != nil {
								logger.Error(errRemove, "Failed delete probe", "probeName", probeName)
								return ctrl.Result{}, errRemove
							}
							return ctrl.Result{}, err
						}
					}
				}
			}
		}
		// update status namespaces after creating probes
		seeOperatorLive.Status.Namespaces = namespacesToMonitor
		err = r.Status().Update(ctx, &seeOperatorLive)
		if err != nil {
			logger.Error(err, "Failed to update SeeOperator status namespaces")
			return ctrl.Result{}, err
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
				for _, ns := range namespacesToMonitor {
					allEndpoints := &corev1.EndpointsList{}
					// FIX: use allEndpoints, remove trailing client.
					err = r.List(ctx, allEndpoints, client.InNamespace(ns))
					if err != nil && apierrors.IsNotFound(err) {
						logger.Info("No Endpoints in namespace", "Namespace", ns)
						continue
					} else if err != nil {
						logger.Error(err, "Can't get endpoints in namespace", "Namespace", ns)
						return ctrl.Result{}, err
					}
					// FIX: iterate endpoints.Items (one Endpoints per service)
					for _, endpoints := range allEndpoints.Items {
						var relatedPods []corev1.Pod
						// FIX: iterate endpoints.Subsets (not endpoints.Items)
						for _, subset := range endpoints.Subsets {
							allAddresses := append(subset.Addresses, subset.NotReadyAddresses...)
							for _, addr := range allAddresses {
								if addr.TargetRef == nil || addr.TargetRef.Kind != "Pod" {
									continue
								}
								pod := &corev1.Pod{}
								// FIX: use r.Get instead of c.Get
								err = r.Get(ctx, client.ObjectKey{
									Name:      addr.TargetRef.Name,
									Namespace: addr.TargetRef.Namespace,
								}, pod)
								if err != nil {
									// FIX: return ctrl.Result{} instead of nil
									return ctrl.Result{}, fmt.Errorf("failed to get pod %s/%s: %w",
										addr.TargetRef.Namespace, addr.TargetRef.Name, err)
								}
								relatedPods = append(relatedPods, *pod)
							}
						}
						// FIX: > 0 instead of > =
						if len(relatedPods) > 0 {
							firstPod := relatedPods[0]
							// FIX: check err == nil (success case), not err != nil
							allLivenessUrls, err := utils.GetLivenessProbesOfPod(ctx, firstPod)
							if err == nil && len(allLivenessUrls) > 0 {
								// FIX: declare variables properly
								probeName := endpoints.Name + "-" + endpoints.Namespace + "-probe"
								probeTargetName := endpoints.Name + "." + endpoints.Namespace + ".svc.cluster.local" + allLivenessUrls[0]
								createdProbe, err := utils.CreateProbe(ctx, r.Client, &seeOperatorLive, *r.Scheme, probeName, probeTargetName, blackboxExporterUrl, endpoints.Namespace, seeOperatorLive.Spec.ProbeSelectorLabels)
								if err != nil {
									logger.Error(err, "Failed Create Probe Reconcile", "probeName", probeName)
									return ctrl.Result{}, err
								}
								seeOperatorLive.Status.ProbeNames = append(seeOperatorLive.Status.ProbeNames, probeName)
								err = r.Status().Update(ctx, &seeOperatorLive)
								if err != nil {
									logger.Error(err, "Failed Update probeNames in operator status", "probeName", probeName)
									errRemove := r.Delete(ctx, createdProbe)
									if errRemove != nil {
										logger.Error(errRemove, "Failed delete probe", "probeName", probeName)
										return ctrl.Result{}, errRemove
									}
									return ctrl.Result{}, err
								}
							}
						}
					}
				}
				// update status namespaces
				seeOperatorLive.Status.Namespaces = namespacesToMonitor
				err = r.Status().Update(ctx, &seeOperatorLive)
				if err != nil {
					logger.Error(err, "Failed to update SeeOperator status namespaces")
					return ctrl.Result{}, err
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
							// FIX: use seeOperatorLive (not seeOperator) and ProbeNames (PascalCase)
							probeIndex := slices.Index(seeOperatorLive.Status.ProbeNames, probe.Name)
							if probeIndex != -1 {
								seeOperatorLive.Status.ProbeNames = slices.Delete(seeOperatorLive.Status.ProbeNames, probeIndex, probeIndex+1)
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
