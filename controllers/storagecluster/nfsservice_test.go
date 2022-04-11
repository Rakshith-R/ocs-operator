package storagecluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	api "github.com/red-hat-storage/ocs-operator/api/v1"
)

func TestNFSService(t *testing.T) {
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
			assertNFSSService(t, reconciler, cr, request)
		}
	}

}

func assertNFSSService(t *testing.T, reconciler StorageClusterReconciler, cr *api.StorageCluster, request reconcile.Request) {
	actualNfsS := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: "ocsinit-cephnfs-service",
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
				"ceph_nfs": "ocsinit-cephnfs",
			},
			SessionAffinity: "ClientIP",
		},
	}
	request.Name = "ocsinit-cephnfs-service"
	err := reconciler.Client.Get(context.TODO(), request.NamespacedName, actualNfsS)
	assert.NoError(t, err)

	expectedAf, err := reconciler.newNFSService(cr)
	assert.NoError(t, err)

	assert.Equal(t, len(expectedAf.OwnerReferences), 1)

	assert.Equal(t, expectedAf.ObjectMeta.Name, actualNfsS.ObjectMeta.Name)
	assert.Equal(t, expectedAf.Spec, actualNfsS.Spec)
}
