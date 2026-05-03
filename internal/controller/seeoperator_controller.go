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

	integreatlyv1alpha1 "github.com/grafana/grafana-operator/v5/api/v1beta1" //For GrafanaDashboard
	seeoperatorv1 "github.com/mohamedbstar413/see-operator/api/v1"
	manifests "github.com/mohamedbstar413/see-operator/internal/manifests"
	"github.com/mohamedbstar413/see-operator/internal/utils"
	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	controllerutil "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	log "sigs.k8s.io/controller-runtime/pkg/log"
)

// SeeOperatorReconciler reconciles a SeeOperator object
type SeeOperatorReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=see-operator.example.com,resources=seeoperators,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=see-operator.example.com,resources=seeoperators/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=see-operator.example.com,resources=seeoperators/finalizers,verbs=update

// ─────────────────────────────────────────────
// HELPER 1 — create a k8s resource if not found
// ─────────────────────────────────────────────

// createIfNotExists sets owner reference and creates the object only when it
// does not already exist in the cluster. It is a no-op (returns nil) when the
// object is already present.
func (r *SeeOperatorReconciler) createIfNotExists(
	ctx context.Context,
	obj client.Object,
	owner *seeoperatorv1.SeeOperator,
) error {
	logger := log.FromContext(ctx)

	if err := ctrl.SetControllerReference(owner, obj, r.Scheme); err != nil {
		return err
	}

	existing := obj.DeepCopyObject().(client.Object)
	err := r.Get(ctx, client.ObjectKeyFromObject(obj), existing)
	if err == nil {
		logger.Info("Resource already exists, skipping creation", "name", obj.GetName())
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}

	logger.Info("Resource not found, creating it", "name", obj.GetName())
	if err = r.Create(ctx, obj); err != nil {
		logger.Error(err, "Failed to create resource", "name", obj.GetName())
		return err
	}
	logger.Info("Created resource successfully", "name", obj.GetName())
	return nil
}

// ─────────────────────────────────────────────
// HELPER 2 — create probes for a single namespace
// ─────────────────────────────────────────────

// createProbesForNamespace lists every Endpoint in ns, resolves the backing
// pods, reads their liveness-probe paths, and creates a Probe CR for each
// service that does not already have one.
func (r *SeeOperatorReconciler) createProbesForNamespace(
	ctx context.Context,
	ns string,
	seeOperatorLive *seeoperatorv1.SeeOperator,
	blackboxExporterUrl string,
) error {
	logger := log.FromContext(ctx)
	namespacedName := client.ObjectKey{Namespace: ns, Name: seeOperatorLive.Name}

	allEndpoints := &corev1.EndpointsList{}
	if err := r.List(ctx, allEndpoints, client.InNamespace(ns)); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("No endpoints found in namespace", "namespace", ns)
			return nil
		}
		return fmt.Errorf("failed to list endpoints in namespace %s: %w", ns, err)
	}

	for _, endpoints := range allEndpoints.Items {
		relatedPods, err := r.resolvePodsFromEndpoints(ctx, endpoints)
		if err != nil {
			return err
		}

		if len(relatedPods) == 0 {
			continue
		}

		allLivenessUrls, err := utils.GetLivenessProbesOfPod(ctx, relatedPods[0])
		if err != nil || len(allLivenessUrls) == 0 {
			continue
		}

		probeName := endpoints.Name + "-" + endpoints.Namespace + "-probe"

		// skip if probe already tracked in status
		if slices.Contains(seeOperatorLive.Status.ProbeNames, probeName) {
			continue
		}

		svcHost := endpoints.Name + "." + endpoints.Namespace + ".svc.cluster.local"
		probeTargetName := "http://" + svcHost + allLivenessUrls[0]
		logger.Info("Creating probe", "probeName", probeName, "target", probeTargetName)

		createdProbe, err := utils.CreateProbe(
			ctx, r.Client, seeOperatorLive, *r.Scheme,
			probeName, probeTargetName, blackboxExporterUrl,
			endpoints.Namespace, seeOperatorLive.Spec.ProbeSelectorLabels,
		)
		if err != nil {
			logger.Error(err, "Failed to create probe", "probeName", probeName)
			return err
		}
		err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
			latest := &seeoperatorv1.SeeOperator{}
			if err := r.Get(ctx, namespacedName, latest); err != nil {
				return err
			}
			latest.Status.ProbeNames = append(seeOperatorLive.Status.ProbeNames, probeName)
			return r.Status().Update(ctx, latest)
		})
		if err != nil {
			logger.Error(err, "Failed to update probe names in status")
			logger.Info("Rolling back probe creation due to status update failure", "probeName", probeName)
			if errRemove := r.Delete(ctx, createdProbe); errRemove != nil {
				logger.Error(errRemove, "Failed to delete probe during rollback", "probeName", probeName)
				return errRemove
			}
			return err
		}
	}

	return nil
}

// ─────────────────────────────────────────────
// HELPER 3 — delete all probes in a namespace
// ─────────────────────────────────────────────

// deleteProbesInNamespace removes every Probe CR in ns and keeps
// seeOperatorLive.Status.ProbeNames in sync.
func (r *SeeOperatorReconciler) deleteProbesInNamespace(
	ctx context.Context,
	ns string,
	seeOperatorLive *seeoperatorv1.SeeOperator,
) error {
	logger := log.FromContext(ctx)

	var probeList monitoringv1.ProbeList
	if err := r.List(ctx, &probeList, client.InNamespace(ns)); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to list probes in namespace %s: %w", ns, err)
	}

	for _, probe := range probeList.Items {
		if err := r.Delete(ctx, &probe); err != nil {
			logger.Error(err, "Failed to delete probe", "probe", probe.Name)
			return err
		}
		if idx := slices.Index(seeOperatorLive.Status.ProbeNames, probe.Name); idx != -1 {
			seeOperatorLive.Status.ProbeNames = slices.Delete(seeOperatorLive.Status.ProbeNames, idx, idx+1)
		}
	}

	logger.Info("Deleted all probes in namespace", "namespace", ns)
	return nil
}

// ─────────────────────────────────────────────
// HELPER 4 — resolve pods backing an Endpoints object
// ─────────────────────────────────────────────

// resolvePodsFromEndpoints walks every subset of an Endpoints object (both
// ready and not-ready addresses) and returns the Pod objects that back it.
func (r *SeeOperatorReconciler) resolvePodsFromEndpoints(
	ctx context.Context,
	endpoints corev1.Endpoints,
) ([]corev1.Pod, error) {
	var relatedPods []corev1.Pod

	for _, subset := range endpoints.Subsets {
		allAddresses := append(subset.Addresses, subset.NotReadyAddresses...)
		for _, addr := range allAddresses {
			if addr.TargetRef == nil || addr.TargetRef.Kind != "Pod" {
				continue
			}
			pod := &corev1.Pod{}
			if err := r.Get(ctx, client.ObjectKey{
				Name:      addr.TargetRef.Name,
				Namespace: addr.TargetRef.Namespace,
			}, pod); err != nil {
				return nil, fmt.Errorf("failed to get pod %s/%s: %w",
					addr.TargetRef.Namespace, addr.TargetRef.Name, err)
			}
			relatedPods = append(relatedPods, *pod)
		}
	}

	return relatedPods, nil
}

// ─────────────────────────────────────────────
// cleanup — remove all owned resources before deletion
// ─────────────────────────────────────────────

func (r *SeeOperatorReconciler) cleanup(
	ctx context.Context,
	seeOperatorLive *seeoperatorv1.SeeOperator,
) error {
	logger := log.FromContext(ctx)
	logger.Info("Running cleanup before deletion")

	// delete all probes across all monitored namespaces
	for _, ns := range seeOperatorLive.Status.Namespaces {
		if err := r.deleteProbesInNamespace(ctx, ns, seeOperatorLive); err != nil {
			return fmt.Errorf("failed to delete probes in namespace %s: %w", ns, err)
		}
	}

	// owned resources (blackbox deployment, service, configmap, cronjob,
	// grafana dashboard CM) are in the same namespace as the CR so
	// Kubernetes garbage-collects them automatically via owner references
	// — no manual deletion needed for those.

	logger.Info("Cleanup completed successfully")
	return nil
}

// ─────────────────────────────────────────────
// RECONCILE — lean main loop
// ─────────────────────────────────────────────

func (r *SeeOperatorReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	const seeOperatorFinalizer = "see-operator.example.com/finalizer"
	seeoperatorv1.AddToScheme(r.Scheme)
	monitoringv1.AddToScheme(r.Scheme)
	integreatlyv1alpha1.AddToScheme(r.Scheme)
	namespacedName := client.ObjectKey{Namespace: req.Namespace, Name: req.Name}

	// ── fetch the SeeOperator CR ──────────────────────────────────────────
	var seeOperatorLive seeoperatorv1.SeeOperator
	if err := r.Get(ctx, req.NamespacedName, &seeOperatorLive); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("SeeOperator not found, probably deleted")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// handle deletion with finalizer
	if !seeOperatorLive.DeletionTimestamp.IsZero() {
		// CR is being deleted
		if controllerutil.ContainsFinalizer(&seeOperatorLive, seeOperatorFinalizer) {
			// run cleanup
			if err := r.cleanup(ctx, &seeOperatorLive); err != nil {
				logger.Error(err, "Failed to run cleanup during deletion")
				return ctrl.Result{}, err
			}
			// remove the finalizer so Kubernetes can delete the CR
			err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
				latest := &seeoperatorv1.SeeOperator{}
				if err := r.Get(ctx, namespacedName, latest); err != nil {
					return err
				}
				controllerutil.RemoveFinalizer(&seeOperatorLive, seeOperatorFinalizer)
				return r.Status().Update(ctx, latest)
			})
			if err != nil {
				logger.Error(err, "Failed to remove finalizer")
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}
	//add finalizer if not existing
	if !controllerutil.ContainsFinalizer(&seeOperatorLive, seeOperatorFinalizer) {
		err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			latest := &seeoperatorv1.SeeOperator{}
			if err := r.Get(ctx, namespacedName, latest); err != nil {
				return err
			}
			controllerutil.AddFinalizer(&seeOperatorLive, seeOperatorFinalizer)
			return r.Status().Update(ctx, latest)
		})
		if err != nil {
			logger.Error(err, "Failed to add finalizer")
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil // requeue after adding finalizer
	}

	// ── resolve blackbox exporter URL ─────────────────────────────────────
	blackboxExporterUrl := seeOperatorLive.Spec.BlackboxUrl

	if blackboxExporterUrl != "" {
		logger.Info("Blackbox exporter URL provided in CRD", "url", blackboxExporterUrl)
		err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			latest := &seeoperatorv1.SeeOperator{}
			if err := r.Get(ctx, namespacedName, latest); err != nil {
				return err
			}
			latest.Status.BlackboxExporterUrl = blackboxExporterUrl
			return r.Status().Update(ctx, latest)
		})
		if err != nil {
			logger.Error(err, "Failed to update blackbox exporter URL in status")
			return ctrl.Result{}, err
		}
	} else {
		// ── deploy built-in blackbox exporter ─────────────────────────────
		logger.Info("No blackbox URL provided, deploying built-in blackbox exporter")
		appsv1.AddToScheme(r.Scheme)
		corev1.AddToScheme(r.Scheme)

		cm := manifests.GetBlackboxCM(r.Scheme)
		cm.Namespace = seeOperatorLive.Namespace
		if err := r.createIfNotExists(ctx, cm, &seeOperatorLive); err != nil {
			return ctrl.Result{}, err
		}

		bbService := manifests.GetBlackboxService(r.Scheme)
		bbService.Namespace = seeOperatorLive.Namespace
		if err := r.createIfNotExists(ctx, bbService, &seeOperatorLive); err != nil {
			return ctrl.Result{}, err
		}

		bbDeployment := manifests.GetBlackboxDeployment(r.Scheme)
		bbDeployment.Namespace = seeOperatorLive.Namespace
		if err := r.createIfNotExists(ctx, bbDeployment, &seeOperatorLive); err != nil {
			return ctrl.Result{}, err
		}

		blackboxExporterUrl = bbService.Name + "." + bbService.Namespace + ".svc.cluster.local:9115"
		err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			latest := &seeoperatorv1.SeeOperator{}
			if err := r.Get(ctx, namespacedName, latest); err != nil {
				return err
			}
			latest.Status.BlackboxExporterUrl = blackboxExporterUrl
			return r.Status().Update(ctx, latest)
		})
		if err != nil {
			logger.Error(err, "Failed to update blackbox exporter URL in status")
			return ctrl.Result{}, err
		}
	}

	// ── ensure CronJob exists ─────────────────────────────────────────────
	cronjob := manifests.GetCronJobYaml(r.Scheme)
	cronjob.Namespace = seeOperatorLive.Namespace
	err := ctrl.SetControllerReference(&seeOperatorLive, cronjob, r.Scheme)
	if err != nil {
		logger.Error(err, "Failed to set controller reference for CronJob")
		return ctrl.Result{}, err
	}
	if err := r.createIfNotExists(ctx, cronjob, &seeOperatorLive); err != nil {
		return ctrl.Result{}, err
	}

	// ── ensure grafana dashboard exists ─────────────────────────────────────────────
	dash := manifests.GetGrafanaDash(r.Scheme)
	dash.Namespace = seeOperatorLive.Namespace
	err = ctrl.SetControllerReference(&seeOperatorLive, dash, r.Scheme)
	if err != nil {
		logger.Error(err, "Failed to set controller reference for Grafana dashboard")
		return ctrl.Result{}, err
	}
	if err := r.createIfNotExists(ctx, dash, &seeOperatorLive); err != nil {
		return ctrl.Result{}, err
	}

	// ── check if any probe is missing (sweep flag) ────────────────────────
	namespacesToMonitor := seeOperatorLive.Spec.NamespacesToMonitor
	statusNamespacesToMonitor := seeOperatorLive.Status.Namespaces

	var sweep bool
	for _, ns := range namespacesToMonitor {
		allEndpoints := &corev1.EndpointsList{}
		if err := r.List(ctx, allEndpoints, client.InNamespace(ns)); err != nil {
			logger.Error(err, "Failed to list endpoints", "namespace", ns)
			return ctrl.Result{}, err
		}

		probeList := &monitoringv1.ProbeList{}
		if err := r.List(ctx, probeList, client.InNamespace(ns)); err != nil {
			logger.Error(err, "Failed to list probes", "namespace", ns)
			return ctrl.Result{}, err
		}

		existingProbeNames := make(map[string]bool)
		for _, p := range probeList.Items {
			existingProbeNames[p.Name] = true
		}

		for _, ep := range allEndpoints.Items {
			if !existingProbeNames[ep.Name+"-"+ep.Namespace+"-probe"] {
				sweep = true
				break
			}
		}
	}

	// ── sweep: create missing probes in already-monitored namespaces ──────
	if sweep {
		logger.Info("Sweep required: creating missing probes")
		for _, ns := range namespacesToMonitor {
			if err := r.createProbesForNamespace(ctx, ns, &seeOperatorLive, blackboxExporterUrl); err != nil {
				return ctrl.Result{}, err
			}
		}
	} else {
		logger.Info("No sweep needed")
	}

	// ── case 1: both nil — nothing to do ─────────────────────────────────
	if namespacesToMonitor == nil && statusNamespacesToMonitor == nil {
		logger.Info("Both namespacesToMonitor and statusNamespacesToMonitor are nil, nothing to do")
		return ctrl.Result{}, nil
	}

	// ── case 2: spec cleared → delete all probes ─────────────────────────
	if namespacesToMonitor == nil && statusNamespacesToMonitor != nil {
		logger.Info("Spec namespaces removed, deleting all probes")
		for _, ns := range statusNamespacesToMonitor {
			if err := r.deleteProbesInNamespace(ctx, ns, &seeOperatorLive); err != nil {
				return ctrl.Result{}, err
			}
		}
		seeOperatorLive.Status.Namespaces = nil
		seeOperatorLive.Status.ProbeNames = nil
		err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			latest := &seeoperatorv1.SeeOperator{}
			if err := r.Get(ctx, namespacedName, latest); err != nil {
				return err
			}
			latest.Status.ProbeNames = nil
			latest.Status.Namespaces = nil
			return r.Status().Update(ctx, latest)
		})
		if err != nil {
			logger.Error(err, "Failed to clear SeeOperator status")
			return ctrl.Result{}, err
		}
		logger.Info("Cleared SeeOperator status")
		return ctrl.Result{}, nil
	}

	// ── case 3: first run — status empty, create probes for all spec ns ──
	if namespacesToMonitor != nil && statusNamespacesToMonitor == nil {
		logger.Info("First run: creating probes for all spec namespaces")
		for _, ns := range namespacesToMonitor {
			if err := r.createProbesForNamespace(ctx, ns, &seeOperatorLive, blackboxExporterUrl); err != nil {
				return ctrl.Result{}, err
			}
		}
		err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			latest := &seeoperatorv1.SeeOperator{}
			if err := r.Get(ctx, namespacedName, latest); err != nil {
				return err
			}
			latest.Status.Namespaces = namespacesToMonitor
			return r.Status().Update(ctx, latest)
		})
		if err != nil {
			logger.Error(err, "Failed to update status namespaces")
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// ── case 4: both non-nil — reconcile differences ─────────────────────
	slices.Sort(namespacesToMonitor)
	slices.Sort(statusNamespacesToMonitor)

	if slices.Equal(namespacesToMonitor, statusNamespacesToMonitor) {
		logger.Info("Spec and status namespaces are equal, nothing to do")
		return ctrl.Result{}, nil
	}

	if len(namespacesToMonitor) > len(statusNamespacesToMonitor) {
		// namespaces added → create probes for new ones
		logger.Info("New namespaces detected in spec, creating probes")
		for _, ns := range namespacesToMonitor {
			if err := r.createProbesForNamespace(ctx, ns, &seeOperatorLive, blackboxExporterUrl); err != nil {
				return ctrl.Result{}, err
			}
		}
	} else {
		// namespaces removed → delete probes for removed ones
		logger.Info("Namespaces removed from spec, deleting their probes")
		for _, statusNs := range statusNamespacesToMonitor {
			if slices.Contains(namespacesToMonitor, statusNs) {
				continue
			}
			if err := r.deleteProbesInNamespace(ctx, statusNs, &seeOperatorLive); err != nil {
				return ctrl.Result{}, err
			}
		}
	}

	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &seeoperatorv1.SeeOperator{}
		if err := r.Get(ctx, namespacedName, latest); err != nil {
			return err
		}
		latest.Status.Namespaces = namespacesToMonitor
		return r.Status().Update(ctx, latest)
	})
	if err != nil {
		logger.Error(err, "Failed to update SeeOperator status namespaces")
		return ctrl.Result{}, err
	}
	logger.Info("Updated SeeOperator status to match spec", "namespaces", namespacesToMonitor)

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
