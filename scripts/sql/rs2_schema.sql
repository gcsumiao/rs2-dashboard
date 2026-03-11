CREATE TABLE IF NOT EXISTS rs2_scan (
  report_id TEXT NOT NULL,
  created_utc TIMESTAMPTZ NOT NULL,
  date_utc DATE NOT NULL,
  source_month TEXT NOT NULL DEFAULT '',
  user_id TEXT NOT NULL DEFAULT '',
  account_id TEXT NOT NULL DEFAULT '',
  email TEXT NOT NULL DEFAULT '',
  report_vin TEXT NOT NULL DEFAULT '',
  vehicle_vin TEXT NOT NULL DEFAULT '',
  vin_final TEXT NOT NULL DEFAULT '',
  zip_postal TEXT NOT NULL DEFAULT '',
  zip5 TEXT NOT NULL DEFAULT '',
  city TEXT NOT NULL DEFAULT '',
  state TEXT NOT NULL DEFAULT '',
  usb_product_id TEXT NOT NULL DEFAULT '',
  customer_id TEXT NOT NULL DEFAULT '',
  vehicle_year TEXT NOT NULL DEFAULT '',
  vehicle_make TEXT NOT NULL DEFAULT '',
  vehicle_model TEXT NOT NULL DEFAULT '',
  buy_from TEXT NOT NULL DEFAULT '',
  tool_name TEXT NOT NULL DEFAULT 'UNMAPPED'
);

ALTER TABLE rs2_scan ADD COLUMN IF NOT EXISTS source_month TEXT NOT NULL DEFAULT '';
ALTER TABLE rs2_scan ADD COLUMN IF NOT EXISTS buy_from TEXT NOT NULL DEFAULT '';

CREATE TABLE IF NOT EXISTS rs2_report_part_counts (
  report_id TEXT PRIMARY KEY,
  abs_parts INTEGER NOT NULL DEFAULT 0,
  srs_parts INTEGER NOT NULL DEFAULT 0,
  cel_parts INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS rs2_buynow_click (
  report_id TEXT NOT NULL,
  click_id TEXT NOT NULL,
  created_utc TIMESTAMPTZ NOT NULL,
  date_utc DATE NOT NULL,
  email TEXT NOT NULL DEFAULT '',
  buy_from TEXT NOT NULL DEFAULT '',
  clicked_account_id TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS rs2_zip_lookup (
  zip5 TEXT PRIMARY KEY,
  city TEXT NOT NULL DEFAULT '',
  state TEXT NOT NULL DEFAULT '',
  lat DOUBLE PRECISION,
  lng DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS rs2_quality (
  id BIGSERIAL PRIMARY KEY,
  generated_at_utc TIMESTAMPTZ NOT NULL,
  mapping_fallback_usb937 TEXT,
  ambiguous_mapping_keys INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS rs2_validation_retailer_month (
  source_month TEXT NOT NULL,
  retailer TEXT NOT NULL,
  expected_unique_report_scans INTEGER NOT NULL DEFAULT 0,
  actual_unique_report_scans INTEGER NOT NULL DEFAULT 0,
  delta INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (source_month, retailer)
);

CREATE INDEX IF NOT EXISTS idx_rs2_scan_date ON rs2_scan(date_utc);
CREATE INDEX IF NOT EXISTS idx_rs2_scan_report ON rs2_scan(report_id);
CREATE INDEX IF NOT EXISTS idx_rs2_scan_user ON rs2_scan(user_id);
CREATE INDEX IF NOT EXISTS idx_rs2_scan_account ON rs2_scan(account_id);
CREATE INDEX IF NOT EXISTS idx_rs2_scan_vin ON rs2_scan(vin_final);
CREATE INDEX IF NOT EXISTS idx_rs2_scan_zip5 ON rs2_scan(zip5);
CREATE INDEX IF NOT EXISTS idx_rs2_scan_zip_postal ON rs2_scan(zip_postal);
CREATE INDEX IF NOT EXISTS idx_rs2_scan_tool ON rs2_scan(tool_name);
CREATE INDEX IF NOT EXISTS idx_rs2_scan_month ON rs2_scan(source_month);
CREATE INDEX IF NOT EXISTS idx_rs2_scan_buy_from ON rs2_scan(buy_from);

CREATE INDEX IF NOT EXISTS idx_rs2_buynow_date ON rs2_buynow_click(date_utc);
CREATE INDEX IF NOT EXISTS idx_rs2_buynow_report ON rs2_buynow_click(report_id);
CREATE INDEX IF NOT EXISTS idx_rs2_buynow_buy_from ON rs2_buynow_click(buy_from);
CREATE INDEX IF NOT EXISTS idx_rs2_buynow_clicked_account ON rs2_buynow_click(clicked_account_id);
