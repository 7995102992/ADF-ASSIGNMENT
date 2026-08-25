# ADF-ASSIGNMENT

================================================================================
PROJECT TECHNICAL SPECIFICATION & IMPLEMENTATION DOCUMENT
================================================================================
Project Title: Metadata-Driven Azure Data Framework & Star Schema Warehouse
Document Version: 2.0 (Incremental Watermark & Data Quality Integrated)
Technology Stack: Azure Data Factory (ADF), Azure Data Lake Storage Gen2, Azure SQL Database, Azure Synapse / SQL Dedicated Pool, T-SQL

--------------------------------------------------------------------------------
1. EXECUTIVE SUMMARY
--------------------------------------------------------------------------------
This project implements an enterprise metadata-driven ETL/ELT data engineering framework on Microsoft Azure. The architecture automates file-based ingestion from Azure Data Lake Storage Gen2 (ADLS Gen2), executes structural and data quality checks, quarantines bad and duplicate data in SQL staging tables, manages incremental delta loading via a high-watermark pattern, and populates an analytical star schema warehouse (Azure Synapse / Dedicated SQL Pool) with Slowly Changing Dimensions (SCD Type 1 & Type 2) and Sales Facts.

--------------------------------------------------------------------------------
2. END-TO-END SOLUTION ARCHITECTURE
--------------------------------------------------------------------------------
1. Landing Layer (ADLS Gen2):
   - Sales files arrive in immutable raw formats (CSV, Parquet, JSON) organized by path patterns: raw/sales/orders/{yyyy}/{MM}/{dd}/

2. Orchestration Layer (Azure Data Factory v2):
   - Master Pipeline (pl_SalesOrchestrator_Main): Looks up active entities and triggers execution per entity.
   - Child Pipeline (PL_Metadata_Driven_Load): Dynamically processes files, ingests raw staging data, evaluates high-watermark delta updates, calls SQL warehouse load procedures, and updates metadata checkpoints.

3. Staging & Data Quality Layer (Azure SQL Database - stg schema):
   - Temporary landing tables: stg.SalesOrder, stg.Customer, stg.Product, stg.Store.
   - Quarantine & Audit tables: stg.ErrorRows (malformed/null records) and stg.DuplicateLog (duplicate natural keys).

4. Presentation Warehouse Layer (Azure Synapse / Dedicated SQL Pool - dw schema):
   - Star Schema optimized with Columnstore Indexes and Hash/Replicated distributions.
   - Reporting Views exposed for Power BI, Excel, and analytical DirectQuery workloads.

--------------------------------------------------------------------------------
3. METADATA CONTROL LAYER (meta Schema)
--------------------------------------------------------------------------------
The framework avoids hardcoded pipelines by driving all operations through metadata control tables:

* meta.SourceSystem:
  - Tracks source ERP systems, connection details, and status.

* meta.EntityConfig:
  - Core driver table containing EntityName, EntityType (Dimension/Fact), SourceContainer, SourcePathPattern, SourceFileFormat, StagingTable, TargetSchema, TargetTable, LoadType, BusinessKeyColumns, WatermarkColumn, LastWatermarkValue, SCDType, and Priority.

* meta.ColumnMapping:
  - Maps source columns to target columns with explicit data types, nullability, ordinal positions, and business key flags.

* meta.ValidationRule:
  - Stores configurable business data quality rules (NotNull, Range, Regex, Referential checks).

* meta.PipelineRunLog & meta.FileLog:
  - Complete execution telemetry capturing ADFRunId, EntityId, status, duration, file checksums, and row counters (RowsRead, RowsInserted, RowsUpdated, RowsRejected).

--------------------------------------------------------------------------------
4. AZURE DATA FACTORY PIPELINE DESIGN
--------------------------------------------------------------------------------
Master Pipeline: pl_SalesOrchestrator_Main
  1. GetActiveEntities: Lookup activity against meta.EntityConfig WHERE IsActive = 1 ORDER BY Priority.
  2. ForEachEntity: Iterates over active entities with parallel or sequential batching.
  3. ExecuteEntityPipeline: Invokes PL_Metadata_Driven_Load passing EntityId.

Child Pipeline: PL_Metadata_Driven_Load
  1. GetEntityConfig (Lookup): Retrieves table metadata, storage path pattern, and watermark column.
  2. GetFiles (Get Metadata): Discovers files present in the ADLS directory.
  3. ForEachFile: Loops through discovered files.
     - CopyFileToStaging (Copy Data): Ingests raw files into staging (stg) tables dynamically.
  4. GetNewWatermark (Lookup):
     - Executes dynamic SQL to capture: MAX(WatermarkColumn) from newly staged data.
     - Safely falls back to current UTC timestamp for dimensions without watermark columns.
  5. ExecuteTargetLoadAndDeduplication (Stored Procedure):
     - Applies validation rules, redirects malformed rows to stg.ErrorRows.
     - Deduplicates business keys using ROW_NUMBER() window functions and logs duplicates to stg.DuplicateLog.
     - Executes MERGE / SCD dimension loads and fact lookups.
  6. UpdateWatermarkValue (Stored Procedure):
     - Calls meta.usp_UpdateWatermark to record the new high watermark in meta.EntityConfig.

--------------------------------------------------------------------------------
5. STAR SCHEMA WAREHOUSE DESIGN (dw Schema)
--------------------------------------------------------------------------------
* dw.DimDate (Conformed / Role-playing):
  - Populated with 10–15 years of calendar attributes.
  - Distribution: REPLICATE, Clustered Index on DateKey (YYYYMMDD).

* dw.DimProduct (SCD Type 1):
  - Overwrites attributes on change while preserving ProductKey surrogate identity.
  - Distribution: REPLICATE, Clustered Index on ProductKey.

* dw.DimCustomer (SCD Type 2):
  - Maintains full customer history using IsCurrent, EffectiveFrom, EffectiveTo, and LastUpdated timestamps.
  - Distribution: REPLICATE, Clustered Index on CustomerKey.

* dw.DimStore:
  - Geographic store hierarchy (Region, City).
  - Distribution: REPLICATE, Clustered Index on StoreKey.

* dw.FactSales:
  - Grain: One row per sales order line item.
  - Foreign Surrogate Keys: DateKey, ProductKey, CustomerKey, StoreKey.
  - Measures: Quantity, UnitPrice, DiscountAmount, NetAmount, CostAmount.
  - Distribution: HASH(CustomerKey), Clustered Columnstore Index.

--------------------------------------------------------------------------------
6. REPORTING LAYER & VIEWS
--------------------------------------------------------------------------------
* dw.vw_SalesDetail:
  - Denormalized view joining FactSales with DimDate, DimProduct, DimCustomer, and DimStore for operational reporting and ad-hoc analysis.

* dw.vw_DailySalesSummary:
  - Aggregated view reporting daily revenue, units sold, distinct orders, and total margin.

* dw.vw_SalesByCategory:
  - Analytical rollup summarizing sales revenue and volume across product categories and sub-categories.

================================================================================

================================================================================
