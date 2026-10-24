use agent_runtime_core::store::VersionedSessionState;
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Debug, Default)]
pub(crate) struct ExtensionState(BTreeMap<String, Arc<VersionedSessionState>>);
impl From<BTreeMap<String, VersionedSessionState>> for ExtensionState {
    fn from(values: BTreeMap<String, VersionedSessionState>) -> Self {
        Self(
            values
                .into_iter()
                .map(|(key, value)| (key, Arc::new(value)))
                .collect(),
        )
    }
}
impl ExtensionState {
    pub(crate) fn get(&self, key: &str) -> Option<&VersionedSessionState> {
        self.0.get(key).map(Arc::as_ref)
    }
    pub(crate) fn get_mut(&mut self, key: &str) -> Option<&mut VersionedSessionState> {
        self.0.get_mut(key).map(Arc::make_mut)
    }
    pub(crate) fn contains_key(&self, key: &str) -> bool {
        self.0.contains_key(key)
    }
    pub(crate) fn insert(
        &mut self,
        key: String,
        value: impl Into<Arc<VersionedSessionState>>,
    ) -> Option<Arc<VersionedSessionState>> {
        self.0.insert(key, value.into())
    }
    pub(crate) fn remove(&mut self, key: &str) -> Option<Arc<VersionedSessionState>> {
        self.0.remove(key)
    }
    pub(crate) fn insert_shared(&mut self, key: String, value: Arc<VersionedSessionState>) {
        self.0.insert(key, value);
    }
    pub(crate) fn iter(&self) -> impl Iterator<Item = (&String, &VersionedSessionState)> {
        self.0.iter().map(|(key, value)| (key, value.as_ref()))
    }
    pub(crate) fn shared(&self) -> BTreeMap<String, Arc<VersionedSessionState>> {
        self.0.clone()
    }
    pub(crate) fn materialize(&self) -> BTreeMap<String, VersionedSessionState> {
        self.iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    }
}
impl Extend<(String, VersionedSessionState)> for ExtensionState {
    fn extend<T: IntoIterator<Item = (String, VersionedSessionState)>>(&mut self, values: T) {
        for (key, value) in values {
            self.insert(key, value);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_runtime_registry::RegistryRevision;

    #[test]
    fn replacing_and_editing_namespaces_keeps_captured_versions_immutable() {
        let value = VersionedSessionState::new(
            RegistryRevision::new("r"),
            serde_json::json!({"n": -0.0, "body": "x".repeat(100_000)}),
        );
        let mut state = ExtensionState::from(BTreeMap::from([("large".to_owned(), value)]));
        let first = state.shared();
        state.insert(
            "other".to_owned(),
            VersionedSessionState::new(RegistryRevision::new("r"), serde_json::Value::Null),
        );
        let second = state.shared();
        assert!(Arc::ptr_eq(&first["large"], &second["large"]));
        state.get_mut("large").unwrap().value["n"] = serde_json::json!(0.0);
        let third = state.shared();
        assert!(!Arc::ptr_eq(&first["large"], &third["large"]));
        assert_eq!(
            first["large"].value["n"].as_f64().unwrap().to_bits(),
            (-0.0f64).to_bits()
        );
        let removed = state.remove("large").unwrap();
        state.insert("restored".to_owned(), removed.clone());
        assert!(Arc::ptr_eq(&state.shared()["restored"], &removed));
    }
}
