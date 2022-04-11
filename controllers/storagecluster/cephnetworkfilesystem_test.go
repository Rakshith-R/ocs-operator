package storagecluster

import (
	"context"
	"testing"

	cephv1 "github.com/rook/rook/pkg/apis/ceph.rook.io/v1"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	api "github.com/red-hat-storage/ocs-operator/api/v1"
	"github.com/red-hat-storage/ocs-operator/controllers/defaults"
)

func TestCephNetworkFileSystem(t *testing.T) {
	var cases = []struct {
		label                string
		createRuntimeObjects bool
	}{
		{
			label:                "case 1",
			createRuntimeObjects: false,
		},
	}
	for _, eachPlatform := range allPlatforms {
		cp := &Platform{platform: eachPlatform}
		for _, c := range cases {
			var objects []client.Object
			t, reconciler, cr, request := initStorageClusterResourceCreateUpdateTestWithPlatform(
				t, cp, objects, nil)
			if c.createRuntimeObjects {
				objects = createUpdateRuntimeObjects(t, cp, reconciler) //nolint:staticcheck //no need to use objects as they update in runtime
			}
			assertCephNetworkFileSystem(t, reconciler, cr, request)
		}
	}

}

func assertCephNetworkFileSystem(t *testing.T, reconciler StorageClusterReconciler, cr *api.StorageCluster, request reconcile.Request) {
	actualNfs := &cephv1.CephNFS{
		ObjectMeta: metav1.ObjectMeta{
			Name: "ocsinit-cephnetworkfilesystem",
		},
		Spec: cephv1.NFSGaneshaSpec{
			Server: cephv1.GaneshaServerSpec{
				Active:            1,
				Placement:         getPlacement(cr, "nfs"),
				Resources:         defaults.GetDaemonResources("nfs", cr.Spec.Resources),
				PriorityClassName: openshiftUserCritical,
			},
		},
	}
	request.Name = "ocsinit-cephnetworkfilesystem"
	err := reconciler.Client.Get(context.TODO(), request.NamespacedName, actualNfs)
	assert.NoError(t, err)

	expectedAf, err := reconciler.newCephNetworkFilesystemInstance(cr)
	assert.NoError(t, err)

	assert.Equal(t, len(expectedAf.OwnerReferences), 1)

	assert.Equal(t, expectedAf.ObjectMeta.Name, actualNfs.ObjectMeta.Name)
	assert.Equal(t, expectedAf.Spec, actualNfs.Spec)
}
