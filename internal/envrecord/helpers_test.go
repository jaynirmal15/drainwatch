package envrecord

import "k8s.io/apimachinery/pkg/runtime"

// toRuntimeObjects narrows the test fixtures to runtime.Object so that the fake
// clientset constructor can take them.
func toRuntimeObjects(in []interface{}) []runtime.Object {
	out := make([]runtime.Object, 0, len(in))
	for _, o := range in {
		if ro, ok := o.(runtime.Object); ok {
			out = append(out, ro)
		}
	}
	return out
}
