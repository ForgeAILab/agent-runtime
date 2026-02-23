use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::UsageMetadata;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PriceOverride {
    pub input: f64,
    pub output: f64,
    #[serde(default)]
    pub cache_read: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct PriceEntry {
    pub input_per_million: f64,
    pub output_per_million: f64,
    pub cache_read_per_million: f64,
}

#[derive(Debug, Clone, Default)]
pub struct PriceTable {
    entries: HashMap<String, PriceEntry>,
}

impl PriceTable {
    pub fn seeded() -> Self {
        let mut table = Self::default();
        // Anthropic
        table.insert("claude-3-5-sonnet", 3.0, 15.0, 0.30);
        table.insert("claude-3-5-haiku", 0.80, 4.0, 0.08);
        table.insert("claude-3-opus", 15.0, 75.0, 1.5);
        // OpenAI
        table.insert("gpt-4o", 2.50, 10.0, 2.50);
        table.insert("gpt-4.1", 2.0, 8.0, 2.0);
        table.insert("gpt-4.1-mini", 0.4, 1.6, 0.4);
        table.insert("gpt-4.1-nano", 0.1, 0.4, 0.1);
        table.insert("o3-mini", 1.1, 4.4, 1.1);
        table
    }

    pub fn insert(
        &mut self,
        model_prefix: impl Into<String>,
        input_per_million: f64,
        output_per_million: f64,
        cache_read_per_million: f64,
    ) {
        self.entries.insert(
            model_prefix.into(),
            PriceEntry {
                input_per_million,
                output_per_million,
                cache_read_per_million,
            },
        );
    }

    pub fn apply_overrides(&mut self, overrides: &HashMap<String, PriceOverride>) {
        for (prefix, ov) in overrides {
            self.insert(
                prefix.clone(),
                ov.input,
                ov.output,
                ov.cache_read.unwrap_or(ov.input),
            );
        }
    }

    pub fn estimate_cost(&self, model: &str, usage: &UsageMetadata) -> Option<f64> {
        let entry = self.match_model(model)?;
        let input = (usage.input_tokens as f64 / 1_000_000.0) * entry.input_per_million;
        let output = (usage.output_tokens as f64 / 1_000_000.0) * entry.output_per_million;
        let cache_read = usage
            .cache_read_tokens
            .map(|tokens| (tokens as f64 / 1_000_000.0) * entry.cache_read_per_million)
            .unwrap_or(0.0);
        Some(input + output + cache_read)
    }

    fn match_model(&self, model: &str) -> Option<&PriceEntry> {
        self.entries
            .iter()
            .filter(|(prefix, _)| model.starts_with(prefix.as_str()))
            .max_by_key(|(prefix, _)| prefix.len())
            .map(|(_, entry)| entry)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seeded_table_calculates_claude_cost() {
        let table = PriceTable::seeded();
        let usage = UsageMetadata {
            input_tokens: 200_000,
            output_tokens: 100_000,
            cache_read_tokens: Some(50_000),
            cache_write_tokens: None,
        };

        let cost = table
            .estimate_cost("claude-3-5-sonnet-20241022", &usage)
            .expect("has price");
        let expected = 0.2 * 3.0 + 0.1 * 15.0 + 0.05 * 0.30;
        assert!((cost - expected).abs() < 1e-9);
    }

    #[test]
    fn seeded_table_calculates_openai_cost() {
        let table = PriceTable::seeded();
        let usage = UsageMetadata {
            input_tokens: 1_000_000,
            output_tokens: 2_000_000,
            cache_read_tokens: None,
            cache_write_tokens: None,
        };

        let cost = table
            .estimate_cost("gpt-4o-2024-11-20", &usage)
            .expect("price");
        assert!((cost - 22.5).abs() < 1e-9);
    }
}
