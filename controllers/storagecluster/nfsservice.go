package storagecluster

import (
	"context"
	"fmt"

	ocsv1 "github.com/red-hat-storage/ocs-operator/api/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type ocsNFSService struct{}

// newNFSService returns the Service instance that should be created on first run.
func (r *StorageClusterReconciler) newNFSService(initData *ocsv1.StorageCluster) (*v1.Service, error) {
	var nfsPort int32 = 2049
	obj := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      generateNameForNFSService(initData),
			Namespace: initData.Namespace,
		},
		Spec: v1.ServiceSpec{
			Ports: []v1.ServicePort{
				{
					Name:       "nfs",
					Port:       nfsPort,
					Protocol:   v1.ProtocolTCP,
					TargetPort: intstr.FromInt(int(nfsPort)),
				},
			},
			Selector: map[string]string{
				"app":      "rook-ceph-nfs",
				"ceph_nfs": generateNameForCephNetworkFilesystem(initData),
			},
			SessionAffinity: "ClientIP",
		},
	}

	if initData.Spec.Network != nil && initData.Spec.Network.HostNetwork {
		obj.Spec.ClusterIP = v1.ClusterIPNone
	}

	err := controllerutil.SetControllerReference(initData, obj, r.Scheme)
	if err != nil {
		r.Log.Error(err, "Unable to set Controller Reference for NFS service.", " NFSService ", klog.KRef(obj.Namespace, obj.Name))
		return nil, err
	}

	return obj, nil
}

// ensureCreated ensures that NFS Service resources exist in the desired
// state.
func (obj *ocsNFSService) ensureCreated(r *StorageClusterReconciler, instance *ocsv1.StorageCluster) (reconcile.Result, error) {
	if instance.Spec.NFS == nil || !instance.Spec.NFS.Enable {
		return obj.ensureDeleted(r, instance)
	}

	nfsService, err := r.newNFSService(instance)
	if err != nil {
		return reconcile.Result{}, err
	}

	existing := v1.Service{}
	err = r.Client.Get(context.TODO(), types.NamespacedName{Name: nfsService.Name, Namespace: nfsService.Namespace}, &existing)
	switch {
	case err == nil:
		if existing.DeletionTimestamp != nil {
			r.Log.Info("Unable to restore NFS Service because it is marked for deletion.", "NFSService", klog.KRef(existing.Namespace, existing.Name))
			return reconcile.Result{}, fmt.Errorf("failed to restore initialization object %s because it is marked for deletion", existing.Name)
		}

		r.Log.Info("Restoring original NFS service.", "NFSService", klog.KRef(nfsService.Namespace, nfsService.Name))
		existing.ObjectMeta.OwnerReferences = nfsService.ObjectMeta.OwnerReferences
		existing.Spec.Ports = nfsService.Spec.Ports
		existing.Spec.Selector = nfsService.Spec.Selector
		existing.Spec.SessionAffinity = nfsService.Spec.SessionAffinity

		err = r.Client.Update(context.TODO(), &existing)
		if err != nil {
			r.Log.Error(err, "Unable to update NFS service.", "NFSService", klog.KRef(nfsService.Namespace, nfsService.Name))
			return reconcile.Result{}, err
		}
	case errors.IsNotFound(err):
		r.Log.Info("Creating NFS service.", "NFSService", klog.KRef(nfsService.Namespace, nfsService.Name))
		err = r.Client.Create(context.TODO(), nfsService)
		if err != nil {
			r.Log.Error(err, "Unable to create NFS service.", "NFSService", klog.KRef(nfsService.Namespace, nfsService.Namespace))
			return reconcile.Result{}, err
		}
	}

	return reconcile.Result{}, nil
}

// ensureDeleted deletes the  NFS Service owned by the StorageCluster
func (obj *ocsNFSService) ensureDeleted(r *StorageClusterReconciler, sc *ocsv1.StorageCluster) (reconcile.Result, error) {
	foundNFSService := &v1.Service{}
	nfsService, err := r.newNFSService(sc)
	if err != nil {
		return reconcile.Result{}, err
	}

	err = r.Client.Get(context.TODO(), types.NamespacedName{Name: nfsService.Name, Namespace: sc.Namespace}, foundNFSService)
	if err != nil {
		if errors.IsNotFound(err) {
			r.Log.Info("Uninstall: NFS Service not found.", "NFSService", klog.KRef(nfsService.Namespace, nfsService.Name))
			return reconcile.Result{}, nil
		}
		r.Log.Error(err, "Uninstall: Unable to retrieve NFS Service.", "NFSService", klog.KRef(nfsService.Namespace, nfsService.Name))
		return reconcile.Result{}, fmt.Errorf("uninstall: Unable to retrieve NFS Service %v: %v", nfsService.Name, err)
	}

	if nfsService.GetDeletionTimestamp().IsZero() {
		r.Log.Info("Uninstall: Deleting NFS Service.", "NFSService", klog.KRef(foundNFSService.Namespace, foundNFSService.Name))
		err = r.Client.Delete(context.TODO(), foundNFSService)
		if err != nil {
			r.Log.Error(err, "Uninstall: Failed to delete NFS Service.", "NFSService", klog.KRef(foundNFSService.Namespace, foundNFSService.Name))
			return reconcile.Result{}, fmt.Errorf("uninstall: Failed to delete NFS Service %v: %v", foundNFSService.Name, err)
		}
	}

	err = r.Client.Get(context.TODO(), types.NamespacedName{Name: nfsService.Name, Namespace: sc.Namespace}, foundNFSService)
	if err != nil {
		if errors.IsNotFound(err) {
			r.Log.Info("Uninstall: NFS Service is deleted.", "NFSService", klog.KRef(nfsService.Namespace, nfsService.Name))
			return reconcile.Result{}, nil
		}
	}

	r.Log.Error(err, "Uninstall: Waiting for NFS Service to be deleted.", "NFSService", klog.KRef(nfsService.Namespace, nfsService.Name))
	return reconcile.Result{}, fmt.Errorf("uninstall: Waiting for NFS Service %v to be deleted", nfsService.Name)
}
