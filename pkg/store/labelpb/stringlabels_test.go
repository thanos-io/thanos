package labelpb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStringLabelsBuilder(t *testing.T) {
	b, err := NewStringsLabelsBuilder(labelSize(&Label{
		Name:  "a",
		Value: "b",
	}) + labelSize(&Label{
		Name:  "d",
		Value: "e",
	}))
	require.NoError(t, err)

	b.Add("a", "b")
	b.Add("d", "e")

	lbls := b.Labels()

	require.Equal(t, `{a="b", d="e"}`, lbls.String())

	ReturnPromLabelsToPool(lbls)

	b, err = NewStringsLabelsBuilder(labelSize(&Label{
		Name:  "a",
		Value: "b",
	}) + labelSize(&Label{
		Name:  "d",
		Value: "e",
	}))
	require.NoError(t, err)

	b.Add("a", "b")
	b.Add("d", "e")

	lbls = b.Labels()

	require.Equal(t, `{a="b", d="e"}`, lbls.String())
}
