package storagecluster

import (
	"context"
	"fmt"

	ocsv1 "github.com/red-hat-storage/ocs-operator/api/v1"
	"github.com/red-hat-storage/ocs-operator/controllers/defaults"
	cephv1 "github.com/rook/rook/pkg/apis/ceph.rook.io/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type ocsCephNetworkFilesystem struct{}

// newcephNetworkFilesystemInstance returns the cephNetworkFilesystem instances that should be created
// on first run.
func (r *StorageClusterReconciler) newCephNetworkFilesystemInstance(initData *ocsv1.StorageCluster) (*cephv1.CephNFS, error) {
	obj := &cephv1.CephNFS{
		ObjectMeta: metav1.ObjectMeta{
			Name:      generateNameForCephNetworkFilesystem(initData),
			Namespace: initData.Namespace,
		},
		Spec: cephv1.NFSGaneshaSpec{
			Server: cephv1.GaneshaServerSpec{
				Active:    1,
				Placement: getPlacement(initData, "nfs"),
				Resources: defaults.GetDaemonResources("nfs", initData.Spec.Resources),
				// set PriorityClassName for the NFS pods
				PriorityClassName: openshiftUserCritical,
			},
		},
	}

	err := controllerutil.SetControllerReference(initData, obj, r.Scheme)
	if err != nil {
		r.Log.Error(err, "Unable to set Controller Reference for cephNetworkFilesystem.", "cephNetworkFilesystem", klog.KRef(obj.Namespace, obj.Name))
		return nil, err
	}

	return obj, nil
}

// ensureCreated ensures that cephNetworkFilesystem resources exist in the desired
// state.
func (obj *ocsCephNetworkFilesystem) ensureCreated(r *StorageClusterReconciler, instance *ocsv1.StorageCluster) (reconcile.Result, error) {
	if instance.Spec.NFS == nil || !instance.Spec.NFS.Enable {
		return obj.ensureDeleted(r, instance)
	}

	cephNetworkFilesystem, err := r.newCephNetworkFilesystemInstance(instance)
	if err != nil {
		return reconcile.Result{}, err
	}

	existing := cephv1.CephNFS{}
	err = r.Client.Get(context.TODO(), types.NamespacedName{Name: cephNetworkFilesystem.Name, Namespace: cephNetworkFilesystem.Namespace}, &existing)
	switch {
	case err == nil:
		if existing.DeletionTimestamp != nil {
			r.Log.Info("Unable to restore cephNetworkFilesystem because it is marked for deletion.", "cephNetworkFilesystem", klog.KRef(existing.Namespace, existing.Name))
			return reconcile.Result{}, fmt.Errorf("failed to restore initialization object %s because it is marked for deletion", existing.Name)
		}

		r.Log.Info("Restoring original cephNetworkFilesystem.", "cephNetworkFilesystem", klog.KRef(cephNetworkFilesystem.Namespace, cephNetworkFilesystem.Name))
		existing.ObjectMeta.OwnerReferences = cephNetworkFilesystem.ObjectMeta.OwnerReferences
		existing.Spec = cephNetworkFilesystem.Spec
		err = r.Client.Update(context.TODO(), &existing)
		if err != nil {
			r.Log.Error(err, "Unable to update cephNetworkFilesystem.", "cephNetworkFilesystem", klog.KRef(cephNetworkFilesystem.Namespace, cephNetworkFilesystem.Name))
			return reconcile.Result{}, err
		}
	case errors.IsNotFound(err):
		r.Log.Info("Creating cephNetworkFilesystem.", "cephNetworkFilesystem", klog.KRef(cephNetworkFilesystem.Namespace, cephNetworkFilesystem.Name))
		err = r.Client.Create(context.TODO(), cephNetworkFilesystem)
		if err != nil {
			r.Log.Error(err, "Unable to create cephNetworkFilesystem.", "cephNetworkFilesystem", klog.KRef(cephNetworkFilesystem.Namespace, cephNetworkFilesystem.Name))
			return reconcile.Result{}, err
		}
	}

	return reconcile.Result{}, nil
}

// ensureDeleted deletes the CephNetworkNetworkFilesystems owned by the StorageCluster
func (obj *ocsCephNetworkFilesystem) ensureDeleted(r *StorageClusterReconciler, sc *ocsv1.StorageCluster) (reconcile.Result, error) {
	foundCephNetworkFilesystem := &cephv1.CephNFS{}
	cephNetworkFilesystem, err := r.newCephNetworkFilesystemInstance(sc)
	if err != nil {
		return reconcile.Result{}, err
	}

	err = r.Client.Get(context.TODO(), types.NamespacedName{Name: cephNetworkFilesystem.Name, Namespace: sc.Namespace}, foundCephNetworkFilesystem)
	if err != nil {
		if errors.IsNotFound(err) {
			r.Log.Info("Uninstall: CephNetworkFileSystem not found.", "CephNetworkFileSystem", klog.KRef(cephNetworkFilesystem.Namespace, cephNetworkFilesystem.Name))
			return reconcile.Result{}, nil
		}
		r.Log.Error(err, "Uninstall: Unable to retrieve CephNetworkFileSystem.", "CephNetworkFileSystem", klog.KRef(cephNetworkFilesystem.Namespace, cephNetworkFilesystem.Name))
		return reconcile.Result{}, fmt.Errorf("uninstall: Unable to retrieve cephNetworkFilesystem %v: %v", cephNetworkFilesystem.Name, err)
	}

	if cephNetworkFilesystem.GetDeletionTimestamp().IsZero() {
		r.Log.Info("Uninstall: Deleting cephNetworkFilesystem.", "CephNetworkFileSystem", klog.KRef(foundCephNetworkFilesystem.Namespace, foundCephNetworkFilesystem.Name))
		err = r.Client.Delete(context.TODO(), foundCephNetworkFilesystem)
		if err != nil {
			r.Log.Error(err, "Uninstall: Failed to delete cephNetworkFilesystem.", "cephNetworkFilesystem", klog.KRef(foundCephNetworkFilesystem.Namespace, foundCephNetworkFilesystem.Name))
			return reconcile.Result{}, fmt.Errorf("uninstall: Failed to delete cephNetworkFilesystem %v: %v", foundCephNetworkFilesystem.Name, err)
		}
	}

	err = r.Client.Get(context.TODO(), types.NamespacedName{Name: cephNetworkFilesystem.Name, Namespace: sc.Namespace}, foundCephNetworkFilesystem)
	if err != nil {
		if errors.IsNotFound(err) {
			r.Log.Info("Uninstall: cephNetworkFilesystem is deleted.", "cephNetworkFilesystem", klog.KRef(cephNetworkFilesystem.Namespace, cephNetworkFilesystem.Name))
			return reconcile.Result{}, nil
		}
	}

	r.Log.Error(err, "Uninstall: Waiting for cephNetworkFilesystem to be deleted.", "cephNetworkFilesystem", klog.KRef(cephNetworkFilesystem.Namespace, cephNetworkFilesystem.Name))
	return reconcile.Result{}, fmt.Errorf("uninstall: Waiting for cephNetworkFilesystem %v to be deleted", cephNetworkFilesystem.Name)
}
