package storagecluster

import (
	"context"
	"fmt"

	ocsv1 "github.com/red-hat-storage/ocs-operator/api/v1"
	"github.com/red-hat-storage/ocs-operator/controllers/defaults"
	cephv1 "github.com/rook/rook/pkg/apis/ceph.rook.io/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type ocsCephNFS struct{}

// newCephNFSInstance returns the cephNFS instance that should be created
// on first run.
func (r *StorageClusterReconciler) newCephNFSInstance(initData *ocsv1.StorageCluster) (*cephv1.CephNFS, error) {
	obj := &cephv1.CephNFS{
		ObjectMeta: metav1.ObjectMeta{
			Name:      generateNameForCephNFS(initData),
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
		r.Log.Error(err, "Unable to set Controller Reference for CephNFS.", "CephNFS", klog.KRef(obj.Namespace, obj.Name))
		return nil, err
	}
	return obj, nil
}

// ensureCreated ensures that cephNFS resource exist in the desired state.
func (obj *ocsCephNFS) ensureCreated(r *StorageClusterReconciler, instance *ocsv1.StorageCluster) (reconcile.Result, error) {
	if instance.Spec.NFS == nil || !instance.Spec.NFS.Enable {
		return obj.ensureDeleted(r, instance)
	}

	cephNFS, err := r.newCephNFSInstance(instance)
	if err != nil {
		return reconcile.Result{}, err
	}
	existingCephNFS := cephv1.CephNFS{}
	err = r.Client.Get(context.TODO(), types.NamespacedName{Name: cephNFS.Name, Namespace: cephNFS.Namespace}, &existingCephNFS)
	switch {
	case err == nil:
		if existingCephNFS.DeletionTimestamp != nil {
			r.Log.Info("Unable to restore CephNFS because it is marked for deletion.", "CephNFS", klog.KRef(existingCephNFS.Namespace, existingCephNFS.Name))
			return reconcile.Result{}, fmt.Errorf("failed to restore initialization object %s because it is marked for deletion", existingCephNFS.Name)
		}

		r.Log.Info("Restoring original CephNFS.", "CephNFS", klog.KRef(cephNFS.Namespace, cephNFS.Name))
		existingCephNFS.ObjectMeta.OwnerReferences = cephNFS.ObjectMeta.OwnerReferences
		existingCephNFS.Spec = cephNFS.Spec
		err = r.Client.Update(context.TODO(), &existingCephNFS)
		if err != nil {
			r.Log.Error(err, "Unable to update CephNFS.", "CephNFS", klog.KRef(cephNFS.Namespace, cephNFS.Name))
			return reconcile.Result{}, err
		}
	case errors.IsNotFound(err):
		r.Log.Info("Creating CephNFS.", "CephNFS", klog.KRef(cephNFS.Namespace, cephNFS.Name))
		err = r.Client.Create(context.TODO(), cephNFS)
		if err != nil {
			r.Log.Error(err, "Unable to create CephNFS.", "CephNFS", klog.KRef(cephNFS.Namespace, cephNFS.Name))
			return reconcile.Result{}, err
		}
	}

	return reconcile.Result{}, nil
}

// ensureDeleted deletes the CephNFS resource owned by the StorageCluster
func (obj *ocsCephNFS) ensureDeleted(r *StorageClusterReconciler, sc *ocsv1.StorageCluster) (reconcile.Result, error) {
	foundCephNFS := &cephv1.CephNFS{}
	cephNFS, err := r.newCephNFSInstance(sc)
	if err != nil {
		return reconcile.Result{}, err
	}

	err = r.Client.Get(context.TODO(), types.NamespacedName{Name: cephNFS.Name, Namespace: sc.Namespace}, foundCephNFS)
	if err != nil {
		if errors.IsNotFound(err) {
			r.Log.Info("Uninstall: CephNFS not found.", "CephNFS", klog.KRef(cephNFS.Namespace, cephNFS.Name))
			return reconcile.Result{}, nil
		}
		r.Log.Error(err, "Uninstall: Unable to retrieve CephNFS.", "CephNFS", klog.KRef(cephNFS.Namespace, cephNFS.Name))
		return reconcile.Result{}, fmt.Errorf("uninstall: Unable to retrieve CephNFS %v: %v", cephNFS.Name, err)
	}

	if cephNFS.GetDeletionTimestamp().IsZero() {
		r.Log.Info("Uninstall: Deleting CephNFS.", "CephNFS", klog.KRef(foundCephNFS.Namespace, foundCephNFS.Name))
		err = r.Client.Delete(context.TODO(), foundCephNFS)
		if err != nil {
			r.Log.Error(err, "Uninstall: Failed to delete CephNFS.", "CephNFS", klog.KRef(foundCephNFS.Namespace, foundCephNFS.Name))
			return reconcile.Result{}, fmt.Errorf("uninstall: Failed to delete CephNFS %v: %v", foundCephNFS.Name, err)
		}
	}

	err = r.Client.Get(context.TODO(), types.NamespacedName{Name: cephNFS.Name, Namespace: sc.Namespace}, foundCephNFS)
	if err != nil {
		if errors.IsNotFound(err) {
			r.Log.Info("Uninstall: CephNFS is deleted.", "CephNFS", klog.KRef(cephNFS.Namespace, cephNFS.Name))
			return reconcile.Result{}, nil
		}
	}
	r.Log.Error(err, "Uninstall: Waiting for CephNFS to be deleted.", "CephNFS", klog.KRef(cephNFS.Namespace, cephNFS.Name))
	return reconcile.Result{}, fmt.Errorf("uninstall: Waiting for CephNFS %v to be deleted", cephNFS.Name)
}

type ocsCephNFSBlockPool struct{}

// newCephNFSBlockPoolInstance returns the cephBlockPool instance required for cephNFS that should be created
// on first run.
func (r *StorageClusterReconciler) newCephNFSBlockPoolInstance(initData *ocsv1.StorageCluster) (*cephv1.CephBlockPool, error) {
	obj := &cephv1.CephBlockPool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      generateNameForCephNFSBlockPool(initData),
			Namespace: initData.Namespace,
		},
		Spec: cephv1.NamedBlockPoolSpec{
			Name: ".nfs",
			PoolSpec: cephv1.PoolSpec{
				FailureDomain:  getFailureDomain(initData),
				Replicated:     generateCephReplicatedSpec(initData, "data"),
				EnableRBDStats: true,
			},
		},
	}

	err := controllerutil.SetControllerReference(initData, obj, r.Scheme)
	if err != nil {
		r.Log.Error(err, "Unable to set controller reference for CephBlockPool.", "CephBlockPool", klog.KRef(obj.Namespace, obj.Name))
		return nil, err
	}

	return obj, nil
}

// ensureCreated ensures that CephNFS related CephBlockPool resource exist in the desired state.
func (obj *ocsCephNFSBlockPool) ensureCreated(r *StorageClusterReconciler, instance *ocsv1.StorageCluster) (reconcile.Result, error) {
	if instance.Spec.NFS == nil || !instance.Spec.NFS.Enable {
		return obj.ensureDeleted(r, instance)
	}

	cephBlockPool, err := r.newCephNFSBlockPoolInstance(instance)
	if err != nil {
		return reconcile.Result{}, err
	}
	existingBlockPool := cephv1.CephBlockPool{}
	err = r.Client.Get(context.TODO(), types.NamespacedName{Name: cephBlockPool.Name, Namespace: cephBlockPool.Namespace}, &existingBlockPool)

	switch {
	case err == nil:
		if existingBlockPool.DeletionTimestamp != nil {
			r.Log.Info("Unable to restore CephBlockPool because it is marked for deletion.", "CephBlockPool", klog.KRef(existingBlockPool.Namespace, existingBlockPool.Name))
			return reconcile.Result{}, fmt.Errorf("failed to restore initialization object %s because it is marked for deletion", existingBlockPool.Name)
		}

		r.Log.Info("Restoring original CephBlockPool.", "CephBlockPool", klog.KRef(cephBlockPool.Namespace, cephBlockPool.Name))
		existingBlockPool.ObjectMeta.OwnerReferences = cephBlockPool.ObjectMeta.OwnerReferences
		existingBlockPool.Spec = cephBlockPool.Spec
		err = r.Client.Update(context.TODO(), &existingBlockPool)
		if err != nil {
			r.Log.Error(err, "Failed to update CephBlockPool.", "CephBlockPool", klog.KRef(cephBlockPool.Namespace, cephBlockPool.Name))
			return reconcile.Result{}, err
		}
	case errors.IsNotFound(err):
		r.Log.Info("Creating CephBlockPool.", "CephBlockPool", klog.KRef(cephBlockPool.Namespace, cephBlockPool.Name))
		err = r.Client.Create(context.TODO(), cephBlockPool)
		if err != nil {
			r.Log.Error(err, "Failed to create CephBlockPool.", "CephBlockPool", klog.KRef(cephBlockPool.Namespace, cephBlockPool.Name))
			return reconcile.Result{}, err
		}
	}

	return reconcile.Result{}, nil
}

// ensureDeleted deletes the CephNFS related CephBlockPool resource owned by the StorageCluster
func (obj *ocsCephNFSBlockPool) ensureDeleted(r *StorageClusterReconciler, sc *ocsv1.StorageCluster) (reconcile.Result, error) {
	foundCephBlockPool := &cephv1.CephBlockPool{}
	cephBlockPool, err := r.newCephNFSBlockPoolInstance(sc)
	if err != nil {
		return reconcile.Result{}, err
	}

	err = r.Client.Get(context.TODO(), types.NamespacedName{Name: cephBlockPool.Name, Namespace: sc.Namespace}, foundCephBlockPool)
	if err != nil {
		if errors.IsNotFound(err) {
			r.Log.Info("Uninstall: CephBlockPool not found.", "CephBlockPool", klog.KRef(cephBlockPool.Namespace, cephBlockPool.Name))
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, fmt.Errorf("uninstall: unable to retrieve CephBlockPool %v: %v", cephBlockPool.Name, err)
	}

	if cephBlockPool.GetDeletionTimestamp().IsZero() {
		r.Log.Info("Uninstall: Deleting CephBlockPool.", "CephBlockPool", klog.KRef(cephBlockPool.Namespace, cephBlockPool.Name))
		err = r.Client.Delete(context.TODO(), foundCephBlockPool)
		if err != nil {
			r.Log.Error(err, "Uninstall: Failed to delete CephBlockPool.", "CephBlockPool", klog.KRef(foundCephBlockPool.Namespace, foundCephBlockPool.Name))
			return reconcile.Result{}, fmt.Errorf("uninstall: Failed to delete CephBlockPool %v: %v", foundCephBlockPool.Name, err)
		}
	}

	err = r.Client.Get(context.TODO(), types.NamespacedName{Name: cephBlockPool.Name, Namespace: sc.Namespace}, foundCephBlockPool)
	if err != nil {
		if errors.IsNotFound(err) {
			r.Log.Info("Uninstall: CephBlockPool is deleted.", "CephBlockPool", klog.KRef(cephBlockPool.Namespace, cephBlockPool.Name))
			return reconcile.Result{}, nil
		}
	}
	r.Log.Error(err, "Uninstall: Waiting for CephBlockPool to be deleted.", "CephBlockPool", klog.KRef(cephBlockPool.Namespace, cephBlockPool.Name))
	return reconcile.Result{}, fmt.Errorf("uninstall: Waiting for CephBlockPool %v to be deleted", cephBlockPool.Name)
}

type ocsCephNFSService struct{}

// newNFSService returns the Service instance that should be created on first run.
func (r *StorageClusterReconciler) newNFSService(initData *ocsv1.StorageCluster) (*v1.Service, error) {
	obj := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      generateNameForNFSService(initData),
			Namespace: initData.Namespace,
		},
		Spec: v1.ServiceSpec{
			Ports: []v1.ServicePort{
				{
					Name: "nfs",
					Port: 2049,
				},
			},
			Selector: map[string]string{
				"app":      "rook-ceph-nfs",
				"ceph_nfs": generateNameForCephNFS(initData),
			},
			SessionAffinity: "ClientIP",
		},
	}

	err := controllerutil.SetControllerReference(initData, obj, r.Scheme)
	if err != nil {
		r.Log.Error(err, "Unable to set Controller Reference for NFS service.", " NFSService ", klog.KRef(obj.Namespace, obj.Name))
		return nil, err
	}

	return obj, nil
}

// ensureCreated ensures that cephNFS related service resource exist in the desired state.
func (obj *ocsCephNFSService) ensureCreated(r *StorageClusterReconciler, instance *ocsv1.StorageCluster) (reconcile.Result, error) {
	if instance.Spec.NFS == nil || !instance.Spec.NFS.Enable {
		return obj.ensureDeleted(r, instance)
	}

	nfsService, err := r.newNFSService(instance)
	if err != nil {
		return reconcile.Result{}, err
	}

	existingNFSService := v1.Service{}
	err = r.Client.Get(context.TODO(), types.NamespacedName{Name: nfsService.Name, Namespace: nfsService.Namespace}, &existingNFSService)
	switch {
	case err == nil:
		if existingNFSService.DeletionTimestamp != nil {
			r.Log.Info("Unable to restore NFS Service because it is marked for deletion.", "NFSService", klog.KRef(existingNFSService.Namespace, existingNFSService.Name))
			return reconcile.Result{}, fmt.Errorf("failed to restore initialization object %s because it is marked for deletion", existingNFSService.Name)
		}

		r.Log.Info("Restoring original NFS service.", "NFSService", klog.KRef(nfsService.Namespace, nfsService.Name))
		existingNFSService.ObjectMeta.OwnerReferences = nfsService.ObjectMeta.OwnerReferences
		existingNFSService.Spec = nfsService.Spec
		err = r.Client.Update(context.TODO(), &existingNFSService)
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

// ensureDeleted deletes the cephNFS related service owned by the StorageCluster
func (obj *ocsCephNFSService) ensureDeleted(r *StorageClusterReconciler, sc *ocsv1.StorageCluster) (reconcile.Result, error) {
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
