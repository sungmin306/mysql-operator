package controller

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	dbv1alpha1 "github.com/sungmin306/mysql-operator/api/v1alpha1"
)

// +kubebuilder:rbac:groups=db.cloudstudy.com,resources=mysqls,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=db.cloudstudy.com,resources=mysqls/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=db.cloudstudy.com,resources=mysqls/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=services;events;persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete

const finalizerName = "mysql.finalizers.db.cloudstudy.com"

type MySQLReconciler struct {
	client.Client
	Recorder record.EventRecorder
	Scheme   *runtime.Scheme
}

func (r *MySQLReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var cr dbv1alpha1.MySQL
	if err := r.Get(ctx, req.NamespacedName, &cr); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !cr.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&cr, finalizerName) {
			logger.Info("MySQL cluster 삭제", "name", cr.Name, "namespace", cr.Namespace)
			r.Recorder.Eventf(&cr, corev1.EventTypeNormal, "Deleted", "MySQL cluster %s/%s 삭제됨", cr.Namespace, cr.Name)

			controllerutil.RemoveFinalizer(&cr, finalizerName)
			if err := r.Update(ctx, &cr); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	if !controllerutil.ContainsFinalizer(&cr, finalizerName) {
		controllerutil.AddFinalizer(&cr, finalizerName)
		if err := r.Update(ctx, &cr); err != nil {
			return ctrl.Result{}, err
		}
		logger.Info("MySQL cluster 생성", "name", cr.Name, "namespace", cr.Namespace, "version", cr.Spec.Version)
		r.Recorder.Eventf(&cr, corev1.EventTypeNormal, "Created",
			"MySQL cluster %s/%s 생성됨 (version=%s)", cr.Namespace, cr.Name, cr.Spec.Version)
	}

	replicas := int32(1)
	if cr.Spec.Replicas != nil {
		replicas = *cr.Spec.Replicas
	}

	if err := r.ensureHeadlessService(ctx, &cr); err != nil {
		return ctrl.Result{}, err
	}

	if err := r.ensureStatefulSet(ctx, &cr, replicas); err != nil {
		return ctrl.Result{}, err
	}

	var sts appsv1.StatefulSet
	if err := r.Get(ctx, types.NamespacedName{Name: cr.Name, Namespace: cr.Namespace}, &sts); err == nil {
		cr.Status.ReadyReplicas = sts.Status.ReadyReplicas
		if sts.Status.ReadyReplicas == replicas {
			cr.Status.Phase = "Ready"
		} else {
			cr.Status.Phase = "Creating"
		}
		_ = r.Status().Update(ctx, &cr)
	}

	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

func (r *MySQLReconciler) ensureHeadlessService(ctx context.Context, cr *dbv1alpha1.MySQL) error {
	name := cr.Name + "-headless"

	var svc corev1.Service
	if err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: cr.Namespace}, &svc); err == nil {
		return nil
	}

	svc = corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cr.Namespace,
			Labels:    map[string]string{"app": cr.Name},
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: corev1.ClusterIPNone,
			Selector:  map[string]string{"app": cr.Name},
			Ports: []corev1.ServicePort{
				{Name: "mysql", Port: 3306},
			},
		},
	}
	if err := controllerutil.SetControllerReference(cr, &svc, r.Scheme); err != nil {
		return err
	}
	return r.Create(ctx, &svc)
}

func (r *MySQLReconciler) ensureStatefulSet(ctx context.Context, cr *dbv1alpha1.MySQL, replicas int32) error {
	key := types.NamespacedName{Name: cr.Name, Namespace: cr.Namespace}

	var sts appsv1.StatefulSet
	if err := r.Get(ctx, key, &sts); err == nil {
		if ptr.Deref(sts.Spec.Replicas, 1) != replicas {
			sts.Spec.Replicas = ptr.To(replicas)
			return r.Update(ctx, &sts)
		}
		return nil
	}

	pvc := corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "data",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{ // ✅ 올바른 타입
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse(cr.Spec.Storage.Size),
				},
			},
			StorageClassName: cr.Spec.Storage.StorageClassName,
		},
	}

	sts = appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cr.Name,
			Namespace: cr.Namespace,
			Labels:    map[string]string{"app": cr.Name},
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: cr.Name + "-headless",
			Replicas:    ptr.To(replicas),
			Selector:    &metav1.LabelSelector{MatchLabels: map[string]string{"app": cr.Name}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": cr.Name}},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:  "mysql",
						Image: fmt.Sprintf("mysql:%s", cr.Spec.Version), // ex: "mysql:8.0"
						Env: []corev1.EnvVar{
							// 테스트용: 빈 비밀번호 허용 (실서비스에서는 Secret 사용)
							{Name: "MYSQL_ALLOW_EMPTY_PASSWORD", Value: "yes"},
						},
						Ports:        []corev1.ContainerPort{{Name: "mysql", ContainerPort: 3306}},
						VolumeMounts: []corev1.VolumeMount{{Name: "data", MountPath: "/var/lib/mysql"}},
					}},
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{pvc},
		},
	}

	if err := controllerutil.SetControllerReference(cr, &sts, r.Scheme); err != nil {
		return err
	}
	return r.Create(ctx, &sts)
}

func (r *MySQLReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.Recorder = mgr.GetEventRecorderFor("mysql-controller")
	r.Scheme = mgr.GetScheme()
	return ctrl.NewControllerManagedBy(mgr).
		For(&dbv1alpha1.MySQL{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Complete(r)
}
