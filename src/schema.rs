use iceberg::spec::{NestedFieldRef, PartitionSpec, Schema, Type};

use crate::error::{Error, Result};

/// Names of the primary-key columns, in canonical (sorted) order.
pub type PrimaryKey = Vec<String>;

/// Assert that both snapshots are diff-compatible:
///
/// - identical schema (names, types, nullability)
/// - identical partition spec (fields + transforms)
/// - both define a primary key (identifier-field-ids) and those PKs match
///
/// Field IDs themselves are ignored because cross-catalog replicas may assign
/// different IDs for the same logical schema. Returns the shared PK column
/// names, which callers use to bucket rows.
pub fn assert_compatible(
    a: &Schema,
    a_spec: &PartitionSpec,
    b: &Schema,
    b_spec: &PartitionSpec,
) -> Result<PrimaryKey> {
    assert_schema_equal(a, b)?;
    assert_partition_equal(a_spec, b_spec)?;
    let pk_a = pk_columns(a, "a")?;
    let pk_b = pk_columns(b, "b")?;
    if pk_a != pk_b {
        return Err(Error::SchemaMismatch(format!(
            "primary key columns differ: {:?} vs {:?}",
            pk_a, pk_b
        )));
    }
    Ok(pk_a)
}

/// Extract and return the primary key column names for a schema, sorted.
/// Fails if the schema has no `identifier-field-ids`.
pub fn pk_columns(s: &Schema, side: &str) -> Result<PrimaryKey> {
    let ids: Vec<i32> = s.identifier_field_ids().collect();
    if ids.is_empty() {
        return Err(Error::MissingPrimaryKey(format!(
            "side {side}: table has no identifier-field-ids; \
             iceberg-diff only supports tables with a primary key"
        )));
    }
    let mut names = Vec::with_capacity(ids.len());
    for id in &ids {
        let field = s.field_by_id(*id).ok_or_else(|| {
            Error::SchemaMismatch(format!(
                "identifier field id {id} not resolvable in schema"
            ))
        })?;
        names.push(field.name.clone());
    }
    names.sort();
    Ok(names)
}

fn assert_schema_equal(a: &Schema, b: &Schema) -> Result<()> {
    let a_fields = a.as_struct().fields();
    let b_fields = b.as_struct().fields();
    if a_fields.len() != b_fields.len() {
        return Err(Error::SchemaMismatch(format!(
            "top-level field count: {} vs {}",
            a_fields.len(),
            b_fields.len()
        )));
    }
    for (fa, fb) in a_fields.iter().zip(b_fields.iter()) {
        assert_field_equal(fa, fb)?;
    }
    Ok(())
}

fn assert_field_equal(a: &NestedFieldRef, b: &NestedFieldRef) -> Result<()> {
    if a.name != b.name {
        return Err(Error::SchemaMismatch(format!(
            "field name: '{}' vs '{}'",
            a.name, b.name
        )));
    }
    if a.required != b.required {
        return Err(Error::SchemaMismatch(format!(
            "field '{}' required: {} vs {}",
            a.name, a.required, b.required
        )));
    }
    assert_type_equal(&a.field_type, &b.field_type, &a.name)?;
    Ok(())
}

fn assert_type_equal(a: &Type, b: &Type, path: &str) -> Result<()> {
    match (a, b) {
        (Type::Primitive(pa), Type::Primitive(pb)) if pa == pb => Ok(()),
        (Type::Struct(sa), Type::Struct(sb)) => {
            let fa = sa.fields();
            let fb = sb.fields();
            if fa.len() != fb.len() {
                return Err(Error::SchemaMismatch(format!(
                    "struct '{}' field count: {} vs {}",
                    path,
                    fa.len(),
                    fb.len()
                )));
            }
            for (x, y) in fa.iter().zip(fb.iter()) {
                assert_field_equal(x, y)?;
            }
            Ok(())
        }
        (Type::List(la), Type::List(lb)) => assert_field_equal(&la.element_field, &lb.element_field),
        (Type::Map(ma), Type::Map(mb)) => {
            assert_field_equal(&ma.key_field, &mb.key_field)?;
            assert_field_equal(&ma.value_field, &mb.value_field)
        }
        _ => Err(Error::SchemaMismatch(format!(
            "type at '{}' differs: {:?} vs {:?}",
            path, a, b
        ))),
    }
}

fn assert_partition_equal(a: &PartitionSpec, b: &PartitionSpec) -> Result<()> {
    let af = a.fields();
    let bf = b.fields();
    if af.len() != bf.len() {
        return Err(Error::PartitionMismatch(format!(
            "field count: {} vs {}",
            af.len(),
            bf.len()
        )));
    }
    for (pa, pb) in af.iter().zip(bf.iter()) {
        if pa.name != pb.name {
            return Err(Error::PartitionMismatch(format!(
                "partition field name: '{}' vs '{}'",
                pa.name, pb.name
            )));
        }
        if pa.transform != pb.transform {
            return Err(Error::PartitionMismatch(format!(
                "transform for '{}': {:?} vs {:?}",
                pa.name, pa.transform, pb.transform
            )));
        }
    }
    Ok(())
}
