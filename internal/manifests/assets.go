package manifests

import (
	"embed"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"

	//for Job and CronJob
	//for ConfigMap, Service.

	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1" //For Probe
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
)

var (
	//go:embed assets/*
	manifests embed.FS
)

func GetProbe(scheme *runtime.Scheme) *monitoringv1.Probe {
	probeBytes, err := manifests.ReadFile("assets/probe-template.yaml")
	if err != nil {
		fmt.Printf("Error Reading Probe template file: %v", err)
		panic(err)
	}
	//create a codecs to transform the bytes to yaml
	codecs := serializer.NewCodecFactory(scheme)
	probeObject, err := runtime.Decode(
		codecs.UniversalDecoder(monitoringv1.SchemeGroupVersion), probeBytes)
	if err != nil {
		fmt.Printf("Error decoding Probe object: %v", err)
		panic(err)
	}
	return probeObject.(*monitoringv1.Probe)
}

func GetCronJobYaml(scheme *runtime.Scheme) *batchv1.CronJob {
	cronJobBytes, err := manifests.ReadFile("assets/cronjob.yaml")
	if err != nil {
		fmt.Printf("Error readding CronJob file %v", err)
		panic(err)
	}
	//create a codecs to transform the bytes to yaml
	codecs := serializer.NewCodecFactory(scheme)
	cronjobObject, err := runtime.Decode(
		codecs.UniversalDecoder(batchv1.SchemeGroupVersion), cronJobBytes)
	if err != nil {
		fmt.Printf("Error decoding CronJob object: %v", err)
		panic(err)
	}
	return cronjobObject.(*batchv1.CronJob)
}

func GetBlackboxCM(scheme *runtime.Scheme) *corev1.ConfigMap {
	cmBytes, err := manifests.ReadFile("assets/blackbox-cm.yaml")
	if err != nil {
		fmt.Printf("Error reading Blackbox ConfigMap file: %v", err)
		panic(err)
	}
	//create a codecs to transform the bytes to yaml
	codecs := serializer.NewCodecFactory(scheme)
	cmObject, err := runtime.Decode(
		codecs.UniversalDecoder(corev1.SchemeGroupVersion), cmBytes)
	if err != nil {
		fmt.Printf("Error decoding ConfigMap object: %v", err)
		panic(err)
	}
	return cmObject.(*corev1.ConfigMap)
}

func GetBlackboxService(scheme *runtime.Scheme) *corev1.Service {
	svcBytes, err := manifests.ReadFile("assets/blackbox-service.yaml")
	if err != nil {
		fmt.Printf("Error reading Blackbox Service file: %v", err)
		panic(err)
	}
	//create a codecs to transform the bytes to yaml
	codecs := serializer.NewCodecFactory(scheme)
	svcObject, err := runtime.Decode(
		codecs.UniversalDecoder(corev1.SchemeGroupVersion), svcBytes)
	if err != nil {
		fmt.Printf("Error decoding Service object: %v", err)
		panic(err)
	}
	return svcObject.(*corev1.Service)
}

func GetBlackboxDeployment(scheme *runtime.Scheme) *appsv1.Deployment {
	deployBytes, err := manifests.ReadFile("assets/blackbox-exporter-template.yaml")
	if err != nil {
		fmt.Printf("Error reading Blackbox Deployment file: %v", err)
		panic(err)
	}
	//create a codecs to transform the bytes to yaml
	codecs := serializer.NewCodecFactory(scheme)
	deployObject, err := runtime.Decode(
		codecs.UniversalDecoder(appsv1.SchemeGroupVersion), deployBytes)
	if err != nil {
		fmt.Printf("Error decoding Deployment object: %v", err)
		panic(err)
	}
	return deployObject.(*appsv1.Deployment)
}
