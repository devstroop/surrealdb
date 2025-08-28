-- =====================================================
-- CORE HEALTHCARE ENTITIES
-- =====================================================

-- Patient Management
create table "Patients"
(
    "Id"                serial
        constraint "PK_Patients"
            primary key,
    "ExternalId"        text unique,
    "FirstName"         text not null,
    "LastName"          text not null,
    "DateOfBirth"       date not null,
    "Gender"            text,
    "Email"             text,
    "Phone"             text,
    -- Deprecated denormalized fields (will be phased out after migration):
    "EmergencyContact"  jsonb,
    "Address"           jsonb,
    "InsuranceInfo"     jsonb,
    "MedicalRecordNumber" text unique,
    "CreatedAt"         timestamp with time zone default now(),
    "UpdatedAt"         timestamp with time zone default now(),
    "IsActive"          boolean default true,
    "ComplianceFlags"   jsonb -- HIPAA, GDPR consent tracking
);

alter table "Patients"
    owner to postgres;

-- Provider Management
create table "Providers"
(
    "Id"                serial
        constraint "PK_Providers"
            primary key,
    "UserId"            text not null,
    "LicenseNumber"     text not null,
    "NPI"               text unique, -- National Provider Identifier
    "Specialties"       text[],
    "Credentials"       jsonb,
    "VerificationStatus" text not null,
    "VerifiedAt"        timestamp with time zone,
    "LicenseExpiry"     date,
    "MalpracticeInsurance" jsonb,
    "ServicesOffered"   text[], -- DEPRECATED: replaced by ProviderServiceTypes
    "AvailabilitySchedule" jsonb, -- DEPRECATED: replaced by ScheduleTemplates & ProviderTimeSlots
    "Ratings"           numeric(3,2),
    "IsActive"          boolean default true,
    "CreatedAt"         timestamp with time zone default now(),
    "UpdatedAt"         timestamp with time zone default now()
);

alter table "Providers"
    owner to postgres;

-- Service Types Configuration
create table "ServiceTypes"
(
    "Id"                serial
        constraint "PK_ServiceTypes"
            primary key,
    "Name"              text not null,
    "Category"          text not null, -- 'on-demand', 'diagnostic', 'telemedicine', etc.
    "Description"       text,
    "RequiredCredentials" text[],
    "ComplianceRequirements" jsonb,
    "DefaultDuration"   integer, -- in minutes
    "PricingModel"      jsonb,
    "IsActive"          boolean default true,
    "CreatedAt"         timestamp with time zone default now(),
    "UpdatedAt"         timestamp with time zone default now()
);

alter table "ServiceTypes"
    owner to postgres;

-- Appointments/Service Requests
create table "Appointments"
(
    "Id"                serial
        constraint "PK_Appointments"
            primary key,
    "PatientId"         integer not null
        constraint "FK_Appointments_Patients_PatientId"
            references "Patients"
            on delete cascade,
    "ProviderId"        integer
        constraint "FK_Appointments_Providers_ProviderId"
            references "Providers"
            on delete set null,
    "ServiceTypeId"     integer not null
        constraint "FK_Appointments_ServiceTypes_ServiceTypeId"
            references "ServiceTypes"
            on delete cascade,
    "Status"            text not null, -- 'scheduled', 'in-progress', 'completed', 'cancelled'
    "ScheduledAt"       timestamp with time zone not null,
    "StartedAt"         timestamp with time zone,
    "CompletedAt"       timestamp with time zone,
    "Location"          jsonb, -- can be virtual, patient home, clinic, etc.
    "Notes"             text,
    "CreatedAt"         timestamp with time zone default now(),
    "UpdatedAt"         timestamp with time zone default now()
);

alter table "Appointments"
    owner to postgres;

-- Medical Records
create table "MedicalRecords"
(
    "Id"                serial
        constraint "PK_MedicalRecords"
            primary key,
    "PatientId"         integer not null
        constraint "FK_MedicalRecords_Patients_PatientId"
            references "Patients"
            on delete cascade,
    "AppointmentId"     integer
        constraint "FK_MedicalRecords_Appointments_AppointmentId"
            references "Appointments"
            on delete set null,
    "ProviderId"        integer
        constraint "FK_MedicalRecords_Providers_ProviderId"
            references "Providers"
            on delete set null,
    "RecordType"        text not null, -- 'consultation', 'lab_result', 'prescription', etc.
    "Content"           jsonb not null, -- Encrypted medical data
    "Attachments"       jsonb,
    "IsEncrypted"       boolean default true,
    "AccessLog"         jsonb[], -- HIPAA audit trail
    "CreatedAt"         timestamp with time zone default now(),
    "UpdatedAt"         timestamp with time zone default now()
);

alter table "MedicalRecords"
    owner to postgres;

-- =====================================================
-- COMPLIANCE & REGULATORY
-- =====================================================

create table "ComplianceAudits"
(
    "Id"                serial
        constraint "PK_ComplianceAudits"
            primary key,
    "EntityType"        text not null, -- 'patient', 'provider', 'appointment', etc.
    "EntityId"          integer not null,
    "Action"            text not null, -- 'view', 'create', 'update', 'delete'
    "UserId"            text not null,
    "IPAddress"         inet,
    "UserAgent"         text,
    "Metadata"          jsonb,
    "Timestamp"         timestamp with time zone default now()
);

alter table "ComplianceAudits"
    owner to postgres;

create table "ConsentRecords"
(
    "Id"                serial
        constraint "PK_ConsentRecords"
            primary key,
    "PatientId"         integer not null
        constraint "FK_ConsentRecords_Patients_PatientId"
            references "Patients"
            on delete cascade,
    "ConsentType"       text not null, -- 'hipaa', 'gdpr', 'marketing', etc.
    "ConsentGiven"      boolean not null,
    "ConsentDate"       timestamp with time zone not null,
    "ExpiryDate"        timestamp with time zone,
    "IPAddress"         inet,
    "DocumentVersion"   text
);

alter table "ConsentRecords"
    owner to postgres;

-- =====================================================
-- OPERATIONAL INTELLIGENCE
-- =====================================================

create table "ServiceMetrics"
(
    "Id"                serial
        constraint "PK_ServiceMetrics"
            primary key,
    "ServiceTypeId"     integer
        constraint "FK_ServiceMetrics_ServiceTypes_ServiceTypeId"
            references "ServiceTypes"
            on delete cascade,
    "ProviderId"        integer
        constraint "FK_ServiceMetrics_Providers_ProviderId"
            references "Providers"
            on delete cascade,
    "Date"              date not null,
    "CompletedCount"    integer default 0,
    "CancelledCount"    integer default 0,
    "AverageRating"     numeric(3,2),
    "AverageDuration"   integer, -- in minutes
    "Revenue"           numeric(10,2),
    "CreatedAt"         timestamp with time zone default now(),
    "UpdatedAt"         timestamp with time zone default now()
);

alter table "ServiceMetrics"
    owner to postgres;

-- (Multi-tenant support removed: previous TenantConfigurations table dropped.)

-- =====================================================
-- INDEXES FOR PERFORMANCE
-- =====================================================

create index "IX_Patients_Email" on "Patients" ("Email");
create index "IX_Providers_UserId" on "Providers" ("UserId");
create index "IX_Providers_VerificationStatus" on "Providers" ("VerificationStatus");
create index "IX_Appointments_PatientId_ScheduledAt" on "Appointments" ("PatientId", "ScheduledAt");
create index "IX_Appointments_ProviderId_ScheduledAt" on "Appointments" ("ProviderId", "ScheduledAt");
create index "IX_Appointments_Status" on "Appointments" ("Status");
create index "IX_MedicalRecords_PatientId" on "MedicalRecords" ("PatientId");
create index "IX_ComplianceAudits_EntityType_EntityId" on "ComplianceAudits" ("EntityType", "EntityId");
create index "IX_ServiceMetrics_Date" on "ServiceMetrics" ("Date");

-- =====================================================
-- STEP 1: DATA INTEGRITY CONSTRAINTS
-- =====================================================

-- Uniqueness (idempotent: use IF NOT EXISTS pattern where supported; for constraints need name checks)
alter table "Providers" add constraint "UQ_Providers_LicenseNumber" unique ("LicenseNumber") not deferrable initially immediate;
alter table "Providers" add constraint "UQ_Providers_UserId" unique ("UserId") not deferrable initially immediate;
alter table "ServiceTypes" add constraint "UQ_ServiceTypes_Category_Name" unique ("Category","Name");

-- Value ranges / logical checks
alter table "Providers" add constraint "CK_Providers_Ratings_Range" check ("Ratings" is null or ("Ratings" >= 0 and "Ratings" <= 5));
alter table "Patients" add constraint "CK_Patients_DateOfBirth_Past" check ("DateOfBirth" < current_date);
alter table "Appointments" add constraint "CK_Appointments_Time_Order" check ("CompletedAt" is null or "StartedAt" is null or "CompletedAt" >= "StartedAt");

-- Prevent negative metrics
alter table "ServiceMetrics" add constraint "CK_ServiceMetrics_NonNegative" check (
    ("CompletedCount" >= 0) and ("CancelledCount" >= 0) and ("AverageDuration" is null or "AverageDuration" >= 0) and ("Revenue" is null or "Revenue" >= 0)
);

-- Ensure license expiry future if active (if expired but IsActive still true will fail)
alter table "Providers" add constraint "CK_Providers_LicenseExpiry_Active" check (
    "LicenseExpiry" is null or NOT ("IsActive" and "LicenseExpiry" < current_date)
);

-- Step 1 note: Added UpdatedAt to core mutable tables and constraints for uniqueness and data quality.

-- =====================================================
-- STEP 2: CONTROLLED VOCABULARIES (ENUM TYPES)
-- =====================================================

-- Create enum types if they do not already exist (idempotent)
do $$ begin
    if not exists (select 1 from pg_type where typname = 'appointment_status') then
        create type appointment_status as enum ('scheduled','in_progress','completed','cancelled');
    end if;
    if not exists (select 1 from pg_type where typname = 'provider_verification_status') then
        create type provider_verification_status as enum ('pending','submitted','verified','rejected','expired');
    end if;
    if not exists (select 1 from pg_type where typname = 'medical_record_type') then
        create type medical_record_type as enum ('consultation','lab_result','prescription','imaging','note');
    end if;
end $$;

-- Convert existing text columns to enums (casts assume values already conform or you'll need data cleanup)
alter table "Appointments"
    alter column "Status" type appointment_status using "Status"::appointment_status;

alter table "Providers"
    alter column "VerificationStatus" type provider_verification_status using "VerificationStatus"::provider_verification_status;

alter table "MedicalRecords"
    alter column "RecordType" type medical_record_type using "RecordType"::medical_record_type;

-- Optional: Add default statuses for new rows
alter table "Appointments" alter column "Status" set default 'scheduled';
alter table "Providers" alter column "VerificationStatus" set default 'pending';
alter table "MedicalRecords" alter column "RecordType" set default 'note';

-- Index adjustments (drop old status index & recreate with enum if desired - existing remains compatible, keep as-is)

-- =====================================================
-- STEP 3: UPDATEDAT TRIGGERS
-- =====================================================

create or replace function set_updated_at() returns trigger as $$
begin
    new."UpdatedAt" = now();
    return new;
end;
$$ language plpgsql;

-- Helper DO block to create trigger if table has UpdatedAt column
do $$
declare r record;
begin
    for r in select t.relname as tbl
             from pg_class t
             join pg_namespace n on n.oid = t.relnamespace
             where n.nspname = 'public'
               and t.relkind = 'r'
               and t.relname in ('Patients','Providers','ServiceTypes','Appointments','MedicalRecords','ServiceMetrics')
    loop
        execute format('drop trigger if exists trg_%I_set_updated_at on %I', lower(r.tbl), r.tbl);
        -- Only add trigger if column exists
        if exists (select 1 from information_schema.columns where table_name = r.tbl and column_name = 'UpdatedAt') then
            execute format('create trigger trg_%I_set_updated_at before update on %I for each row execute function set_updated_at()', lower(r.tbl), r.tbl);
        end if;
    end loop;
end $$;

-- Step 3 note: Added UpdatedAt to MedicalRecords and created automatic update triggers for mutable tables.

-- =====================================================
-- STEP 4: NORMALIZATION (ADDRESSES, INSURANCE, CONTACTS)
-- =====================================================

-- Reference tables for structured patient-related info. Keep additive; do not drop legacy json yet.

create table if not exists "Addresses" (
    "Id"            serial primary key,
    "Line1"         text not null,
    "Line2"         text,
    "City"          text not null,
    "State"         text,
    "PostalCode"    text,
    "Country"       text default 'US',
    "Latitude"      numeric(9,6),
    "Longitude"     numeric(9,6),
    "Raw"           jsonb, -- original unparsed input
    "CreatedAt"     timestamptz default now(),
    "UpdatedAt"     timestamptz default now()
);

create table if not exists "PatientAddresses" (
    "PatientId"     integer not null references "Patients" on delete cascade,
    "AddressId"     integer not null references "Addresses" on delete cascade,
    "Type"          text not null, -- home, mailing, billing, service
    "IsPrimary"     boolean default false,
    "ValidFrom"     date,
    "ValidTo"       date,
    "CreatedAt"     timestamptz default now(),
    "UpdatedAt"     timestamptz default now(),
    primary key ("PatientId","AddressId","Type")
);

create table if not exists "PatientInsurancePolicies" (
    "Id"            serial primary key,
    "PatientId"     integer not null references "Patients" on delete cascade,
    "Carrier"       text not null,
    "PolicyNumber"  text not null,
    "GroupNumber"   text,
    "PlanType"      text, -- PPO, HMO, etc
    "EffectiveDate" date,
    "ExpiryDate"    date,
    "Metadata"      jsonb,
    "CreatedAt"     timestamptz default now(),
    "UpdatedAt"     timestamptz default now(),
    unique ("PatientId","PolicyNumber")
);

create table if not exists "PatientContacts" (
    "Id"            serial primary key,
    "PatientId"     integer not null references "Patients" on delete cascade,
    "Name"          text not null,
    "Relationship"  text, -- mother, father, sibling, spouse, caregiver
    "Type"          text not null, -- emergency, caregiver, guardian
    "Phone"         text,
    "Email"         text,
    "AddressId"     integer references "Addresses" on delete set null,
    "Preferred"     boolean default false,
    "CreatedAt"     timestamptz default now(),
    "UpdatedAt"     timestamptz default now()
);

-- Triggers for new tables reuse existing function if present
do $$ begin
    if exists (select 1 from pg_proc where proname = 'set_updated_at') then
        perform 1;
    else
        create or replace function set_updated_at() returns trigger as $$
        begin new."UpdatedAt" = now(); return new; end; $$ language plpgsql;
    end if;
end $$;

drop trigger if exists trg_addresses_set_updated_at on "Addresses";
create trigger trg_addresses_set_updated_at before update on "Addresses" for each row execute function set_updated_at();

drop trigger if exists trg_patientaddresses_set_updated_at on "PatientAddresses";
create trigger trg_patientaddresses_set_updated_at before update on "PatientAddresses" for each row execute function set_updated_at();

drop trigger if exists trg_patientinsurance_set_updated_at on "PatientInsurancePolicies";
create trigger trg_patientinsurance_set_updated_at before update on "PatientInsurancePolicies" for each row execute function set_updated_at();

drop trigger if exists trg_patientcontacts_set_updated_at on "PatientContacts";
create trigger trg_patientcontacts_set_updated_at before update on "PatientContacts" for each row execute function set_updated_at();

-- Suggested migration (manual execution outside this static DDL): extract distinct addresses & insurance from legacy json fields.
-- After successful migration & validation, consider dropping Patients.Address / Patients.InsuranceInfo / Patients.EmergencyContact.

-- Indexes to optimize lookups
create index if not exists "IX_PatientAddresses_PatientId_Type" on "PatientAddresses" ("PatientId","Type") where "IsPrimary" = true;
create index if not exists "IX_PatientInsurancePolicies_PatientId_Active" on "PatientInsurancePolicies" ("PatientId") where ("ExpiryDate" is null or "ExpiryDate" >= current_date);
create index if not exists "IX_PatientContacts_PatientId_Type" on "PatientContacts" ("PatientId","Type");

-- Step 4 note: Introduced normalized address, insurance, contact tables; legacy json columns retained for backfill period.

-- =====================================================
-- STEP 5: SECURITY & COMPLIANCE HARDENING
-- =====================================================

-- 5.1 Rename ComplianceAudits -> AuditEvents (idempotent logic)
do $$ begin
    if exists (select 1 from information_schema.tables where table_name='ComplianceAudits')
       and not exists (select 1 from information_schema.tables where table_name='AuditEvents') then
        alter table "ComplianceAudits" rename to "AuditEvents";
    end if;
end $$;

-- Drop old index if table renamed, then recreate with new name
do $$ begin
    if exists (select 1 from pg_class where relname='IX_ComplianceAudits_EntityType_EntityId') then
        execute 'drop index "IX_ComplianceAudits_EntityType_EntityId"';
    end if;
end $$;

-- 5.2 Enum for audit outcome
do $$ begin
    if not exists (select 1 from pg_type where typname='audit_outcome') then
        create type audit_outcome as enum ('success','failure','partial');
    end if;
end $$;

-- 5.3 Add enhanced columns to AuditEvents
alter table "AuditEvents" add column if not exists "CorrelationId" uuid;
alter table "AuditEvents" add column if not exists "Outcome" audit_outcome default 'success'::audit_outcome;
alter table "AuditEvents" add column if not exists "ErrorCode" text;
alter table "AuditEvents" add column if not exists "ActorRole" text; -- role at time of action

-- 5.4 Recreate indexes under new naming
create index if not exists "IX_AuditEvents_EntityType_EntityId" on "AuditEvents" ("EntityType","EntityId");
create index if not exists "IX_AuditEvents_CorrelationId" on "AuditEvents" ("CorrelationId");
create index if not exists "IX_AuditEvents_Timestamp" on "AuditEvents" ("Timestamp" desc);

-- 5.5 MedicalRecords security metadata
alter table "MedicalRecords" add column if not exists "ContentHash" text; -- application populated (e.g. SHA256 of encrypted blob)
alter table "MedicalRecords" add column if not exists "KeyVersion" integer; -- encryption key version reference
alter table "MedicalRecords" add column if not exists "Sensitivity" smallint default 0; -- 0=low,1=standard,2=moderate,3=high,4=restricted,5=sealed
alter table "MedicalRecords" add constraint if not exists "CK_MedicalRecords_Sensitivity_Range" check ("Sensitivity" between 0 and 5);

-- 5.6 Index to speed retrieval respecting sensitivity & recency
create index if not exists "IX_MedicalRecords_PatientId_Sensitivity_CreatedAt" on "MedicalRecords" ("PatientId","Sensitivity","CreatedAt" desc);

-- 5.7 Helper function to log audit events uniformly
create or replace function log_audit_event(
    p_entity_type text,
    p_entity_id integer,
    p_action text,
    p_user_id text,
    p_outcome audit_outcome default 'success',
    p_error_code text default null,
    p_actor_role text default null,
    p_correlation_id uuid default null,
    p_metadata jsonb default '{}'::jsonb
) returns void as $$
begin
    insert into "AuditEvents" ("EntityType","EntityId","Action","UserId","Outcome","ErrorCode","ActorRole","CorrelationId","Metadata")
    values (p_entity_type, p_entity_id, p_action, p_user_id, p_outcome, p_error_code, p_actor_role, p_correlation_id, p_metadata);
end; $$ language plpgsql security definer;

comment on function log_audit_event(text,integer,text,text,audit_outcome,text,text,uuid,jsonb) is 'Unified insertion point for audit logging with outcome classification';

-- 5.8 (Optional) RLS scaffolding (disabled by default) - commented for later activation
-- alter table "MedicalRecords" enable row level security;
-- create policy medicalrecords_patient_access on "MedicalRecords"
--     using (true); -- Replace with actual predicate referencing current_user or JWT claims

-- Step 5 note: Renamed audit table, added outcome & correlation metadata, added security metadata to MedicalRecords, created logging helper.

-- =====================================================
-- STEP 6: PERFORMANCE & INDEX TUNING
-- =====================================================
-- Goal: Optimize high-frequency query patterns while avoiding redundant or bloated indexes.

-- 6.1 Appointments scheduling & operational dashboards
create index if not exists "IX_Appointments_Status_ScheduledAt" on "Appointments" ("Status","ScheduledAt");
create index if not exists "IX_Appointments_Provider_Status_ScheduledAt" on "Appointments" ("ProviderId","Status","ScheduledAt" desc) where "Status" in ('scheduled','in_progress');
create index if not exists "IX_Appointments_Patient_Status_ScheduledAt" on "Appointments" ("PatientId","Status","ScheduledAt" desc);

-- 6.2 Providers active lookup & verification queues
create index if not exists "IX_Providers_IsActive_VerificationStatus" on "Providers" ("IsActive","VerificationStatus") where "IsActive" = true;

-- 6.3 MedicalRecords provider/time access (existing patient-focused indexes retained)
create index if not exists "IX_MedicalRecords_Provider_CreatedAt" on "MedicalRecords" ("ProviderId","CreatedAt" desc);

-- 6.4 ServiceMetrics analytic access (ensure efficient filtering by provider or service over date ranges)
create index if not exists "IX_ServiceMetrics_Provider_Date" on "ServiceMetrics" ("ProviderId","Date");

-- 6.5 AuditEvents correlation and failure triage (Timestamp index already descending; add failure-focused partial)
create index if not exists "IX_AuditEvents_Failures" on "AuditEvents" ("Timestamp" desc) where "Outcome" <> 'success';

-- 6.6 BRIN indexes for large append-only tables (efficient for time-ordered data). Safe no-op if already small; revisit as volume grows.
do $$ begin
    -- Only create BRIN if table exists and not already present.
    if exists (select 1 from information_schema.tables where table_name='AuditEvents')
       and not exists (select 1 from pg_class where relname='BRIN_AuditEvents_Timestamp') then
        execute 'create index "BRIN_AuditEvents_Timestamp" on "AuditEvents" using brin ("Timestamp") with (pages_per_range = 64)';
    end if;
    if exists (select 1 from information_schema.tables where table_name='MedicalRecords')
       and not exists (select 1 from pg_class where relname='BRIN_MedicalRecords_CreatedAt') then
        execute 'create index "BRIN_MedicalRecords_CreatedAt" on "MedicalRecords" using brin ("CreatedAt") with (pages_per_range = 64)';
    end if;
end $$;

-- 6.7 Housekeeping: consider dropping now-redundant simple indexes if composite supersedes them (manual evaluation suggested):
--   IX_Appointments_Status (could be replaced by IX_Appointments_Status_ScheduledAt if single-column not needed)
-- Keep for now to avoid regression; review with pg_stat_user_indexes after production workload sampling.

-- Step 6 note: Added composite, partial, and BRIN indexes to support common query paths and large-scale growth.

-- =====================================================
-- STEP 7: PARTITION & RETENTION SCAFFOLDING
-- =====================================================
-- Non-invasive: adds retention policy table + enforcement function. Full table partitioning left as future opt-in.

create table if not exists "DataRetentionPolicies" (
    "TableName"         text primary key,            -- target table (case-sensitive as stored)
    "RetainDays"        integer not null check ("RetainDays" > 0), -- hard delete threshold
    "ArchiveAfterDays"  integer check ("ArchiveAfterDays" is null or ("ArchiveAfterDays" >= "RetainDays")),
    "ArchiveTarget"     text,                       -- external storage reference (S3 bucket path, etc.)
    "Enabled"           boolean default true,
    "CreatedAt"         timestamptz default now(),
    "UpdatedAt"         timestamptz default now()
);

drop trigger if exists trg_dataretentionpolicies_set_updated_at on "DataRetentionPolicies";
create trigger trg_dataretentionpolicies_set_updated_at before update on "DataRetentionPolicies" for each row execute function set_updated_at();

comment on table "DataRetentionPolicies" is 'Defines per-table data retention & optional archival thresholds.';

-- Seed example policies (idempotent upserts)
insert into "DataRetentionPolicies" ("TableName","RetainDays","ArchiveAfterDays","ArchiveTarget")
    values ('AuditEvents', 365, 1095, 's3://audit-archive/'),
           ('ServiceMetrics', 730, null, null)
on conflict ("TableName") do nothing;

-- Retention enforcement function: deletes rows older than RetainDays based on a detected time column.
create or replace function enforce_retention(p_dry_run boolean default true)
returns table(table_name text, deleted_count bigint, cutoff timestamptz) as $$
declare
    r record;
    v_time_col text;
    v_sql text;
    v_cutoff timestamptz;
    v_deleted bigint;
begin
    for r in select * from "DataRetentionPolicies" where "Enabled" loop
        -- Identify a timestamp column heuristically (override manually if needed)
        select column_name into v_time_col
        from information_schema.columns
        where table_name = r."TableName" and column_name in ('Timestamp','CreatedAt','UpdatedAt','Date')
        order by case column_name when 'Timestamp' then 1 when 'CreatedAt' then 2 when 'UpdatedAt' then 3 when 'Date' then 4 end
        limit 1;
        if v_time_col is null then
            continue; -- skip, no time column
        end if;
        v_cutoff := now() - (r."RetainDays" || ' days')::interval;
        v_sql := format('delete from %I where %I < $1', r."TableName", v_time_col);
        if p_dry_run then
            -- Count only
            execute format('select count(*) from %I where %I < $1', r."TableName", v_time_col) using v_cutoff into v_deleted;
        else
            execute v_sql using v_cutoff; GET DIAGNOSTICS v_deleted = ROW_COUNT;
        end if;
        return query select r."TableName", coalesce(v_deleted,0), v_cutoff;
    end loop;
end; $$ language plpgsql;

comment on function enforce_retention(boolean) is 'Executes (or simulates) deletions per DataRetentionPolicies; pass false to perform deletion.';

-- Suggested scheduling (example, not executed here):
-- Using pg_cron: select cron.schedule('retention_daily','15 2 * * *','select enforce_retention(false);');

-- Partitioning Guidance (not applied automatically):
-- To partition a high-volume table later (e.g., AuditEvents by month):
-- 1. Create new partitioned table structure
--    create table "AuditEvents_part" (like "AuditEvents" including all) partition by range ("Timestamp");
-- 2. Create monthly partitions:
--    create table if not exists "AuditEvents_2025_08" partition of "AuditEvents_part" for values from ('2025-08-01') to ('2025-09-01');
-- 3. COPY data, swap names inside a transaction (downtime or careful lock strategy), then attach trigger-based ingest or view.
-- Alternatively, rebuild via logical replication for near-zero downtime.

-- Step 7 note: Added retention policy infrastructure & documented partition approach without intrusive structural changes.

-- =====================================================
-- STEP 8: OUTBOX / INTEGRATION EVENTS
-- =====================================================
-- Reliable event publishing pattern: write domain changes + outbox row in same transaction; external worker publishes.

-- 8.1 Enum for outbox status
do $$ begin
    if not exists (select 1 from pg_type where typname='outbox_status') then
        create type outbox_status as enum ('pending','processing','published','failed');
    end if;
end $$;

-- 8.2 Outbox table
create table if not exists "OutboxEvents" (
    "Id"              bigserial primary key,
    "EventType"       text not null,              -- e.g. appointment.created
    "AggregateType"   text not null,              -- e.g. Appointment, MedicalRecord
    "AggregateId"     bigint not null,
    "Payload"         jsonb not null,             -- serialized event data (domain object snapshot / delta)
    "OccurredAt"      timestamptz not null default now(),
    "Status"          outbox_status not null default 'pending',
    "ProcessedAt"     timestamptz,
    "RetryCount"      integer not null default 0,
    "ErrorMessage"    text,
    "CorrelationId"   uuid,
    "CreatedAt"       timestamptz default now(),
    "UpdatedAt"       timestamptz default now()
);

comment on table "OutboxEvents" is 'Transactional outbox for reliable integration event publishing.';

create index if not exists "IX_OutboxEvents_Status_OccurredAt" on "OutboxEvents" ("Status","OccurredAt");
create index if not exists "IX_OutboxEvents_Aggregate" on "OutboxEvents" ("AggregateType","AggregateId");

-- 8.3 Trigger for UpdatedAt reuse
drop trigger if exists trg_outboxevents_set_updated_at on "OutboxEvents";
create trigger trg_outboxevents_set_updated_at before update on "OutboxEvents" for each row execute function set_updated_at();

-- 8.4 Helper function to enqueue event (security definer to allow controlled inserts)
create or replace function enqueue_outbox_event(
    p_event_type text,
    p_aggregate_type text,
    p_aggregate_id bigint,
    p_payload jsonb,
    p_correlation_id uuid default null
) returns bigint as $$
declare v_id bigint; begin
    insert into "OutboxEvents" ("EventType","AggregateType","AggregateId","Payload","CorrelationId")
        values (p_event_type, p_aggregate_type, p_aggregate_id, coalesce(p_payload,'{}'::jsonb), p_correlation_id)
        returning "Id" into v_id;
    return v_id; end; $$ language plpgsql security definer;

comment on function enqueue_outbox_event(text,text,bigint,jsonb,uuid) is 'Insert an outbox event row for later publication; returns new Id.';

-- 8.5 (Optional) domain triggers to auto-enqueue events – commented for teams to enable selectively
-- Example: Appointment creation
-- create or replace function trg_appointments_outbox() returns trigger as $$
-- begin
--     perform enqueue_outbox_event('appointment.created','Appointment', NEW."Id", to_jsonb(NEW), null);
--     return NEW;
-- end; $$ language plpgsql;
-- drop trigger if exists appointments_outbox_insert on "Appointments";
-- create trigger appointments_outbox_insert after insert on "Appointments" for each row execute function trg_appointments_outbox();

-- Example: MedicalRecord added
-- create or replace function trg_medicalrecords_outbox() returns trigger as $$
-- begin
--     perform enqueue_outbox_event('medical_record.created','MedicalRecord', NEW."Id", jsonb_build_object('Id',NEW."Id",'PatientId',NEW."PatientId",'RecordType',NEW."RecordType",'CreatedAt',NEW."CreatedAt"), null);
--     return NEW;
-- end; $$ language plpgsql;
-- drop trigger if exists medicalrecords_outbox_insert on "MedicalRecords";
-- create trigger medicalrecords_outbox_insert after insert on "MedicalRecords" for each row execute function trg_medicalrecords_outbox();

-- 8.6 Retention policy seed for outbox (keep 30 days by default)
insert into "DataRetentionPolicies" ("TableName","RetainDays") values ('OutboxEvents', 30)
    on conflict ("TableName") do nothing;

-- Step 8 note: Added OutboxEvents infrastructure with helper enqueue function and optional domain triggers (commented).

-- =====================================================
-- STEP 9: SOFT DELETE & ARCHIVAL STRATEGY
-- =====================================================
-- Adds logical deletion columns to core domain tables so records can be retired without immediate physical removal.
-- Physical cleanup later handled via retention (Step 7) or ad‑hoc archival jobs.

alter table "Patients"              add column if not exists "DeletedAt" timestamptz; alter table "Patients"              add column if not exists "DeletedBy" text;
alter table "Providers"             add column if not exists "DeletedAt" timestamptz; alter table "Providers"             add column if not exists "DeletedBy" text;
alter table "ServiceTypes"          add column if not exists "DeletedAt" timestamptz; alter table "ServiceTypes"          add column if not exists "DeletedBy" text;
alter table "Appointments"          add column if not exists "DeletedAt" timestamptz; alter table "Appointments"          add column if not exists "DeletedBy" text;
alter table "MedicalRecords"        add column if not exists "DeletedAt" timestamptz; alter table "MedicalRecords"        add column if not exists "DeletedBy" text;
alter table "PatientAddresses"      add column if not exists "DeletedAt" timestamptz; alter table "PatientAddresses"      add column if not exists "DeletedBy" text;
alter table "PatientInsurancePolicies" add column if not exists "DeletedAt" timestamptz; alter table "PatientInsurancePolicies" add column if not exists "DeletedBy" text;
alter table "PatientContacts"       add column if not exists "DeletedAt" timestamptz; alter table "PatientContacts"       add column if not exists "DeletedBy" text;

-- Optional check: if DeletedAt set, mark IsActive false where such a column exists (enforced via trigger per-table if desired).
-- For now we rely on application layer or scheduled job.

-- Partial indexes for active (non-deleted) lookups
create index if not exists "IX_Patients_Active_NonDeleted" on "Patients" ("IsActive") where "DeletedAt" is null;
create index if not exists "IX_Providers_Active_NonDeleted" on "Providers" ("IsActive","VerificationStatus") where "DeletedAt" is null and "IsActive" = true;
create index if not exists "IX_ServiceTypes_Active_NonDeleted" on "ServiceTypes" ("IsActive") where "DeletedAt" is null;
create index if not exists "IX_Appointments_NonDeleted_Status" on "Appointments" ("Status","ScheduledAt") where "DeletedAt" is null;
create index if not exists "IX_MedicalRecords_NonDeleted" on "MedicalRecords" ("PatientId","CreatedAt" desc) where "DeletedAt" is null;

-- Generic soft delete helper (whitelist protects against arbitrary table targeting)
create or replace function soft_delete_record(p_table text, p_id bigint, p_user text) returns boolean as $$
declare
    v_sql text;
    v_count int;
begin
    if p_table not in ('Patients','Providers','ServiceTypes','Appointments','MedicalRecords','PatientAddresses','PatientInsurancePolicies','PatientContacts') then
        raise exception 'Soft delete not allowed for table %', p_table;
    end if;
    v_sql := format('update %I set "DeletedAt" = now(), "DeletedBy" = $1 where "Id" = $2 and "DeletedAt" is null', p_table);
    execute v_sql using p_user, p_id; GET DIAGNOSTICS v_count = ROW_COUNT;
    return v_count = 1;
end; $$ language plpgsql security definer;

comment on function soft_delete_record(text,bigint,text) is 'Logical delete: sets DeletedAt/DeletedBy for whitelisted tables. Returns true if one row updated.';

-- View examples (kept simple; applications can also embed WHERE DeletedAt IS NULL):
create or replace view "ActivePatients" as select * from "Patients" where "DeletedAt" is null;
create or replace view "ActiveProviders" as select * from "Providers" where "DeletedAt" is null;

-- Step 9 note: Introduced DeletedAt/DeletedBy, partial indexes for active rows, generic soft delete function, and convenience views.

-- =====================================================
-- STEP 10: EXTENDED INFRASTRUCTURE DOMAINS
-- =====================================================
-- Adds Payments, Scheduling/Dispatch, Communication, Integration, White-Label, Workflow, Facilities & Quality modules.

-- 10.1 ENUM TYPES (idempotent)
do $$ begin
    if not exists (select 1 from pg_type where typname='invoice_status') then
        create type invoice_status as enum ('draft','issued','paid','void','refunded','partial');
    end if;
    if not exists (select 1 from pg_type where typname='payment_method_type') then
        create type payment_method_type as enum ('card','ach','insurance','other');
    end if;
    if not exists (select 1 from pg_type where typname='claim_status') then
        create type claim_status as enum ('pending','submitted','accepted','rejected','denied','paid','partial');
    end if;
    if not exists (select 1 from pg_type where typname='notification_channel') then
        create type notification_channel as enum ('email','sms','push');
    end if;
    if not exists (select 1 from pg_type where typname='notification_status') then
        create type notification_status as enum ('pending','scheduled','sending','sent','failed','cancelled');
    end if;
    if not exists (select 1 from pg_type where typname='timeslot_status') then
        create type timeslot_status as enum ('available','booked','blocked','held');
    end if;
end $$;

-- 10.2 PAYMENT & BILLING
create table if not exists "PaymentMethods" (
    "Id"              serial primary key,
    "PatientId"       integer not null references "Patients" on delete cascade,
    "Type"            payment_method_type not null,
    "ProcessorToken"  text, -- tokenized reference (encrypted externally)
    "Last4"           text,
    "ExpiryMonth"     smallint,
    "ExpiryYear"      smallint,
    "IsDefault"       boolean default false,
    "Metadata"        jsonb,
    "CreatedAt"       timestamptz default now(),
    "UpdatedAt"       timestamptz default now(),
    "DeletedAt"       timestamptz,
    "DeletedBy"       text
);

create table if not exists "Invoices" (
    "Id"              serial primary key,
    "PatientId"       integer not null references "Patients" on delete restrict,
    "AppointmentId"   integer references "Appointments" on delete set null,
    "Status"          invoice_status not null default 'draft',
    "Currency"        text not null default 'USD',
    "SubTotal"        numeric(12,2) default 0,
    "Tax"             numeric(12,2) default 0,
    "Total"           numeric(12,2) default 0,
    "PaidAmount"      numeric(12,2) default 0,
    "DueDate"         date,
    "IssuedAt"        timestamptz,
    "PaidAt"          timestamptz,
    "Metadata"        jsonb,
    "CreatedAt"       timestamptz default now(),
    "UpdatedAt"       timestamptz default now(),
    "DeletedAt"       timestamptz,
    "DeletedBy"       text
);

create table if not exists "InvoiceLineItems" (
    "Id"            serial primary key,
    "InvoiceId"     integer not null references "Invoices" on delete cascade,
    "ServiceTypeId" integer references "ServiceTypes" on delete set null,
    "Description"   text not null,
    "Quantity"      numeric(10,2) not null default 1,
    "UnitPrice"     numeric(12,2) not null default 0,
    "LineTotal"     numeric(12,2) generated always as (coalesce("Quantity",0)*coalesce("UnitPrice",0)) stored,
    "Metadata"      jsonb,
    "CreatedAt"     timestamptz default now(),
    "UpdatedAt"     timestamptz default now()
);

create table if not exists "InsuranceClaims" (
    "Id"            serial primary key,
    "AppointmentId" integer references "Appointments" on delete set null,
    "PolicyId"      integer references "PatientInsurancePolicies" on delete set null,
    "ClaimNumber"   text unique,
    "Status"        claim_status not null default 'pending',
    "SubmittedAt"   timestamptz,
    "ResponseAt"    timestamptz,
    "ApprovedAmount" numeric(12,2),
    "DenialReason"  text,
    "Metadata"      jsonb,
    "CreatedAt"     timestamptz default now(),
    "UpdatedAt"     timestamptz default now()
);

-- 10.3 SERVICE AREAS & FACILITIES
create table if not exists "ServiceAreas" (
    "Id"          serial primary key,
    "Name"        text not null,
    "Type"        text, -- radius, polygon, zipcode
    "Definition"  jsonb not null, -- geojson / zip list
    "IsActive"    boolean default true,
    "CreatedAt"   timestamptz default now(),
    "UpdatedAt"   timestamptz default now()
);

create table if not exists "Facilities" (
    "Id"            serial primary key,
    "Name"          text not null,
    "Type"          text, -- clinic, hospital, mobile, virtual
    "AddressId"     integer references "Addresses" on delete set null,
    "ServiceAreaId" integer references "ServiceAreas" on delete set null,
    "Coordinates"   point,
    "Capacity"      jsonb,
    "IsActive"      boolean default true,
    "CreatedAt"     timestamptz default now(),
    "UpdatedAt"     timestamptz default now()
);

alter table "Appointments" add column if not exists "FacilityId" integer references "Facilities" on delete set null;

-- 10.4 PROVIDER ⇄ SERVICE RELATION
create table if not exists "ProviderServiceTypes" (
    "ProviderId"    integer not null references "Providers" on delete cascade,
    "ServiceTypeId" integer not null references "ServiceTypes" on delete cascade,
    "ServiceAreaId" integer references "ServiceAreas" on delete set null,
    "BaseRate"      numeric(12,2),
    "IsActive"      boolean default true,
    "CreatedAt"     timestamptz default now(),
    "UpdatedAt"     timestamptz default now(),
    primary key ("ProviderId","ServiceTypeId")
);

-- 10.5 SCHEDULING (Templates & Slots)
create table if not exists "ScheduleTemplates" (
    "Id"          serial primary key,
    "ProviderId"  integer not null references "Providers" on delete cascade,
    "DayOfWeek"   smallint not null check ("DayOfWeek" between 0 and 6),
    "StartTime"   time not null,
    "EndTime"     time not null,
    "SlotDuration" integer not null default 30,
    "ServiceAreaId" integer references "ServiceAreas" on delete set null,
    "CreatedAt"   timestamptz default now(),
    "UpdatedAt"   timestamptz default now(),
    unique ("ProviderId","DayOfWeek","StartTime","EndTime")
);

create table if not exists "ProviderTimeSlots" (
    "Id"            bigserial primary key,
    "ProviderId"    integer not null references "Providers" on delete cascade,
    "ServiceTypeId" integer references "ServiceTypes" on delete set null,
    "FacilityId"    integer references "Facilities" on delete set null,
    "StartTime"     timestamptz not null,
    "EndTime"       timestamptz not null,
    "Status"        timeslot_status not null default 'available',
    "AppointmentId" integer references "Appointments" on delete set null,
    "CreatedAt"     timestamptz default now(),
    "UpdatedAt"     timestamptz default now(),
    constraint "CK_ProviderTimeSlots_TimeOrder" check ("EndTime" > "StartTime")
);

-- 10.6 COMMUNICATION / NOTIFICATIONS
create table if not exists "NotificationTemplates" (
    "Id"          serial primary key,
    "Name"        text not null unique,
    "Type"        notification_channel not null,
    "Subject"     text,
    "Body"        text not null,
    "Variables"   text[],
    "IsActive"    boolean default true,
    "CreatedAt"   timestamptz default now(),
    "UpdatedAt"   timestamptz default now()
);

create table if not exists "NotificationQueue" (
    "Id"            bigserial primary key,
    "RecipientId"   integer,
    "RecipientType" text, -- patient / provider
    "TemplateId"    integer references "NotificationTemplates" on delete set null,
    "Channel"       notification_channel not null,
    "Status"        notification_status not null default 'pending',
    "ScheduledFor"  timestamptz,
    "SentAt"        timestamptz,
    "Variables"     jsonb,
    "Error"         text,
    "CreatedAt"     timestamptz default now(),
    "UpdatedAt"     timestamptz default now()
);

-- 10.7 INTEGRATION CONFIGURATION & LOGGING
create table if not exists "IntegrationConfigs" (
    "Id"            serial primary key,
    "Name"          text not null unique,
    "Type"          text not null, -- ehr, lab, pharmacy, insurance
    "Provider"      text, -- epic, cerner, labcorp
    "Credentials"   jsonb, -- encrypted at rest
    "Endpoints"     jsonb,
    "MappingRules"  jsonb,
    "IsActive"      boolean default true,
    "LastSyncAt"    timestamptz,
    "CreatedAt"     timestamptz default now(),
    "UpdatedAt"     timestamptz default now()
);

create table if not exists "IntegrationLogs" (
    "Id"           bigserial primary key,
    "ConfigId"     integer references "IntegrationConfigs" on delete set null,
    "Direction"    text, -- inbound / outbound
    "EntityType"   text,
    "EntityId"     integer,
    "Status"       text,
    "Request"      jsonb,
    "Response"     jsonb,
    "Error"        text,
    "CreatedAt"    timestamptz default now()
);

-- 10.8 ORGANIZATION / WHITE-LABEL & WORKFLOW
create table if not exists "OrganizationSettings" (
    "Id"        serial primary key,
    "Key"       text not null unique,
    "Value"     jsonb not null,
    "Category"  text, -- branding, workflow, features
    "UpdatedAt"  timestamptz default now(),
    "CreatedAt"  timestamptz default now()
);

create table if not exists "WorkflowDefinitions" (
    "Id"          serial primary key,
    "Name"        text not null unique,
    "EntityType"  text not null, -- appointment, provider_onboarding, claim
    "States"      jsonb not null,
    "Transitions" jsonb not null,
    "IsDefault"   boolean default false,
    "CreatedAt"   timestamptz default now(),
    "UpdatedAt"   timestamptz default now()
);

-- 10.9 QUALITY & COMPLIANCE EXTENSIONS
create table if not exists "QualityMetrics" (
    "Id"          bigserial primary key,
    "MetricType"  text not null,
    "EntityType"  text not null,
    "EntityId"    integer not null,
    "Score"       numeric(5,2),
    "Dimensions"  jsonb,
    "MeasuredAt"  timestamptz default now(),
    "CreatedAt"   timestamptz default now()
);

create table if not exists "ComplianceChecks" (
    "Id"           bigserial primary key,
    "CheckType"    text not null,
    "EntityType"   text not null,
    "EntityId"     integer not null,
    "Status"       text, -- passed, failed, warning
    "Details"      jsonb,
    "NextCheckDue" date,
    "CheckedAt"    timestamptz default now(),
    "CreatedAt"    timestamptz default now()
);

-- 10.10 INDEXES FOR NEW TABLES
create index if not exists "IX_Invoices_Status_DueDate" on "Invoices" ("Status","DueDate");
create index if not exists "IX_Invoices_PatientId_Status" on "Invoices" ("PatientId","Status");
create index if not exists "IX_InvoiceLineItems_InvoiceId" on "InvoiceLineItems" ("InvoiceId");
create index if not exists "IX_InsuranceClaims_Status" on "InsuranceClaims" ("Status");
create index if not exists "IX_ProviderServiceTypes_Active" on "ProviderServiceTypes" ("IsActive") where "IsActive" = true;
create index if not exists "IX_ProviderTimeSlots_Provider_Start" on "ProviderTimeSlots" ("ProviderId","StartTime") where "Status" = 'available';
create index if not exists "IX_NotificationQueue_Status_ScheduledFor" on "NotificationQueue" ("Status","ScheduledFor");
create index if not exists "IX_IntegrationLogs_ConfigId_CreatedAt" on "IntegrationLogs" ("ConfigId","CreatedAt" desc);
create index if not exists "IX_QualityMetrics_Entity" on "QualityMetrics" ("EntityType","EntityId","MeasuredAt" desc);
create index if not exists "IX_ComplianceChecks_Entity_NextCheck" on "ComplianceChecks" ("EntityType","EntityId","NextCheckDue");

-- 10.11 TRIGGERS (reuse set_updated_at)
do $$ begin
    perform 1 from pg_proc where proname='set_updated_at'; -- already exists earlier
end $$;

-- Attach to new tables (dynamic loop for brevity)
do $$
declare t text;
begin
    for t in select unnest(array[
        'PaymentMethods','Invoices','InvoiceLineItems','InsuranceClaims','ServiceAreas','Facilities',
        'ProviderServiceTypes','ScheduleTemplates','ProviderTimeSlots','NotificationTemplates',
        'NotificationQueue','IntegrationConfigs','OrganizationSettings','WorkflowDefinitions'
    ]) loop
        execute format('drop trigger if exists trg_%s_updated on "%s"', lower(t), t);
        if exists (select 1 from information_schema.columns where table_name= t and column_name='UpdatedAt') then
            execute format('create trigger trg_%s_updated before update on "%s" for each row execute function set_updated_at()', lower(t), t);
        end if;
    end loop;
end $$;

-- 10.12 RETENTION POLICY SEEDS (non-destructive)
insert into "DataRetentionPolicies" ("TableName","RetainDays") values
    ('IntegrationLogs', 90),
    ('NotificationQueue', 30)
on conflict ("TableName") do nothing;

-- 10.13 VIEWS FOR COMMON QUERIES
create or replace view "UpcomingAppointments" as
    select a.* from "Appointments" a
    where a."Status" in ('scheduled','in_progress') and a."DeletedAt" is null and a."ScheduledAt" >= now()
    order by a."ScheduledAt" asc;

create or replace view "AvailableTimeSlots" as
    select s.* from "ProviderTimeSlots" s
    where s."Status" = 'available' and s."StartTime" >= now() and s."DeletedAt" is null;

-- Step 10 note: Added extended domain tables (payments, claims, scheduling, communication, integrations, workflow, quality) with indexes and triggers.

-- =====================================================
-- PATCH SET A: INTEGRITY & HISTORY ENHANCEMENTS
-- =====================================================

-- A1. Exclusion constraint to prevent overlapping provider time slots (requires btree_gist)
create extension if not exists btree_gist;
alter table "ProviderTimeSlots"
    add column if not exists "DeletedAt" timestamptz,
    add column if not exists "DeletedBy" text;
alter table "ProviderTimeSlots"
    drop constraint if exists "EXCL_ProviderTimeSlots_NoOverlap";
alter table "ProviderTimeSlots"
    add constraint "EXCL_ProviderTimeSlots_NoOverlap"
    exclude using gist (
        "ProviderId" with =,
        tstzrange("StartTime","EndTime") with &&
    ) where ("DeletedAt" is null);

-- A2. Financial ledger and payment transactions
do $$ begin
    if not exists (select 1 from pg_type where typname='payment_tx_type') then
        create type payment_tx_type as enum ('charge','refund','adjustment');
    end if;
    if not exists (select 1 from pg_type where typname='payment_tx_status') then
        create type payment_tx_status as enum ('initiated','authorized','captured','settled','failed','refunded','voided');
    end if;
end $$;

create table if not exists "PaymentTransactions" (
    "Id"              bigserial primary key,
    "InvoiceId"       integer references "Invoices" on delete set null,
    "PaymentMethodId" integer references "PaymentMethods" on delete set null,
    "Type"            payment_tx_type not null,
    "Status"          payment_tx_status not null default 'initiated',
    "Amount"          numeric(12,2) not null,
    "Currency"        text not null default 'USD',
    "ProcessorRef"    text,
    "ProcessorMeta"   jsonb,
    "OccurredAt"      timestamptz default now(),
    "SettledAt"       timestamptz,
    "CreatedAt"       timestamptz default now(),
    "UpdatedAt"       timestamptz default now()
);

create index if not exists "IX_PaymentTransactions_Invoice" on "PaymentTransactions" ("InvoiceId","Status");

-- A3. Invoice integrity math constraint
alter table "Invoices" drop constraint if exists "CK_Invoices_TotalMath";
alter table "Invoices" add constraint "CK_Invoices_TotalMath" check (round(coalesce("SubTotal",0) + coalesce("Tax",0),2) = round(coalesce("Total",0),2));

-- A4. Status history tables
create table if not exists "AppointmentStatusHistory" (
    "Id"            bigserial primary key,
    "AppointmentId" integer not null references "Appointments" on delete cascade,
    "OldStatus"     appointment_status,
    "NewStatus"     appointment_status not null,
    "ChangedBy"     text,
    "Reason"        text,
    "ChangedAt"     timestamptz default now()
);
create index if not exists "IX_AppointmentStatusHistory_App" on "AppointmentStatusHistory" ("AppointmentId","ChangedAt" desc);

create table if not exists "InvoiceStatusHistory" (
    "Id"         bigserial primary key,
    "InvoiceId"  integer not null references "Invoices" on delete cascade,
    "OldStatus"  invoice_status,
    "NewStatus"  invoice_status not null,
    "ChangedBy"  text,
    "Reason"     text,
    "ChangedAt"  timestamptz default now()
);
create index if not exists "IX_InvoiceStatusHistory_Inv" on "InvoiceStatusHistory" ("InvoiceId","ChangedAt" desc);

create table if not exists "ClaimStatusHistory" (
    "Id"         bigserial primary key,
    "ClaimId"    integer not null references "InsuranceClaims" on delete cascade,
    "OldStatus"  claim_status,
    "NewStatus"  claim_status not null,
    "ChangedBy"  text,
    "Reason"     text,
    "ChangedAt"  timestamptz default now()
);
create index if not exists "IX_ClaimStatusHistory_Claim" on "ClaimStatusHistory" ("ClaimId","ChangedAt" desc);

-- A5. Consent revocation support
alter table "ConsentRecords" add column if not exists "RevokedAt" timestamptz;
alter table "ConsentRecords" add column if not exists "RevocationReason" text;
create index if not exists "IX_ConsentRecords_Active" on "ConsentRecords" ("PatientId","ConsentType") where "RevokedAt" is null and "ConsentGiven" = true;

-- A6. Outbox enhancements (schema version + pending index)
alter table "OutboxEvents" add column if not exists "SchemaVersion" integer default 1;
create index if not exists "IX_OutboxEvents_Pending_Id" on "OutboxEvents" ("Id") where "Status" = 'pending';

-- A7. Partial unique index for active MedicalRecordNumber if reuse allowed after deletion
do $$ begin
    if not exists (select 1 from pg_class where relname = 'UQ_Patients_MRN_Active') then
        execute 'create unique index "UQ_Patients_MRN_Active" on "Patients" ("MedicalRecordNumber") where "DeletedAt" is null';
    end if;
end $$;

-- A8. Appointment to ProviderTimeSlot link (optional binding)
alter table "Appointments" add column if not exists "ProviderTimeSlotId" bigint references "ProviderTimeSlots" on delete set null;
create index if not exists "IX_Appointments_ProviderTimeSlot" on "Appointments" ("ProviderTimeSlotId");

-- A9. Trigger to record appointment status history automatically
create or replace function trg_appointment_status_history() returns trigger as $$
begin
    if TG_OP = 'UPDATE' and NEW."Status" is distinct from OLD."Status" then
        insert into "AppointmentStatusHistory" ("AppointmentId","OldStatus","NewStatus","ChangedBy","Reason")
        values (OLD."Id", OLD."Status", NEW."Status", current_setting('app.current_user_id', true), null);
    end if;
    return NEW;
end; $$ language plpgsql;

drop trigger if exists appointment_status_history on "Appointments";
create trigger appointment_status_history after update on "Appointments" for each row execute function trg_appointment_status_history();

-- A10. Placeholder trigger for invoice status history (manual population during transitions)
create or replace function trg_invoice_status_history() returns trigger as $$
begin
    if TG_OP = 'UPDATE' and NEW."Status" is distinct from OLD."Status" then
        insert into "InvoiceStatusHistory" ("InvoiceId","OldStatus","NewStatus","ChangedBy")
        values (OLD."Id", OLD."Status", NEW."Status", current_setting('app.current_user_id', true));
    end if;
    return NEW;
end; $$ language plpgsql;

drop trigger if exists invoice_status_history on "Invoices";
create trigger invoice_status_history after update on "Invoices" for each row execute function trg_invoice_status_history();

-- A11. Claim status history trigger
create or replace function trg_claim_status_history() returns trigger as $$
begin
    if TG_OP = 'UPDATE' and NEW."Status" is distinct from OLD."Status" then
        insert into "ClaimStatusHistory" ("ClaimId","OldStatus","NewStatus","ChangedBy")
        values (OLD."Id", OLD."Status", NEW."Status", current_setting('app.current_user_id', true));
    end if;
    return NEW;
end; $$ language plpgsql;

drop trigger if exists claim_status_history on "InsuranceClaims";
create trigger claim_status_history after update on "InsuranceClaims" for each row execute function trg_claim_status_history();

-- Patch Set A complete.

-- =====================================================
-- PATCH SET B: SECURITY, ENCRYPTION & RLS SCAFFOLDING
-- =====================================================

-- B1. Enum types for key & data classification
do $$ begin
    if not exists (select 1 from pg_type where typname='key_status') then
        create type key_status as enum ('pending','active','retiring','retired','compromised');
    end if;
    if not exists (select 1 from pg_type where typname='data_classification') then
        create type data_classification as enum ('public','internal','pii','phi','secret');
    end if;
end $$;

-- B2. Encryption key registry (manages versions referenced by MedicalRecords.KeyVersion)
create table if not exists "EncryptionKeys" (
    "KeyVersion"     integer generated by default as identity primary key,
    "KMSAlias"       text not null,
    "Status"         key_status not null default 'pending',
    "CreatedAt"      timestamptz not null default now(),
    "ActivatedAt"    timestamptz,
    "RetiredAt"      timestamptz,
    "CompromisedAt"  timestamptz,
    "Metadata"       jsonb,
    constraint "CK_EncryptionKeys_Lifecycle" check (
        case
            when "Status" = 'active' then "ActivatedAt" is not null
            when "Status" = 'retired' then "RetiredAt" is not null
            when "Status" = 'compromised' then "CompromisedAt" is not null
            else true
        end
    )
);

create index if not exists "IX_EncryptionKeys_Status" on "EncryptionKeys" ("Status");

-- B3. Add FK from MedicalRecords.KeyVersion (only if column exists and no FK yet)
do $$ begin
    if exists (select 1 from information_schema.columns where table_name='MedicalRecords' and column_name='KeyVersion') then
        if not exists (
            select 1 from information_schema.constraint_column_usage where table_name='MedicalRecords' and column_name='KeyVersion'
            and constraint_name ilike '%keyversion%') then
            alter table "MedicalRecords" add constraint "FK_MedicalRecords_EncryptionKeys_KeyVersion" foreign key ("KeyVersion") references "EncryptionKeys" ("KeyVersion") on update cascade on delete restrict;
        end if;
    end if;
end $$;

create index if not exists "IX_MedicalRecords_KeyVersion" on "MedicalRecords" ("KeyVersion") where "KeyVersion" is not null;

-- B4. Field / column classification catalog
create table if not exists "FieldClassifications" (
    "Id"             bigserial primary key,
    "TableName"      text not null,
    "ColumnName"     text not null,
    "Classification" data_classification not null,
    "Sensitivity"    smallint not null default 0 check ("Sensitivity" between 0 and 10),
    "Notes"          text,
    "CreatedAt"      timestamptz default now(),
    unique ("TableName","ColumnName")
);

-- Seed core PHI / PII classifications (idempotent)
insert into "FieldClassifications" ("TableName","ColumnName","Classification","Sensitivity") values
    ('Patients','FirstName','pii',2),
    ('Patients','LastName','pii',2),
    ('Patients','DateOfBirth','phi',5),
    ('Patients','MedicalRecordNumber','phi',6),
    ('MedicalRecords','Content','phi',9),
    ('MedicalRecords','RecordType','phi',4)
on conflict ("TableName","ColumnName") do nothing;

-- B5. RLS SCAFFOLDING (safe, permissive policies) ---------------------------------
-- NOTE: Enabling RLS can block access; we add broad allow policies first, then refine.
-- To activate stricter policies later, replace or drop the broad ones.

do $$ begin
    -- Enable RLS only if not already enabled
    if exists (select 1 from information_schema.tables where table_name='Patients') then
        execute 'alter table "Patients" enable row level security';
        execute 'alter table "Patients" force row level security';
    end if;
    if exists (select 1 from information_schema.tables where table_name='MedicalRecords') then
        execute 'alter table "MedicalRecords" enable row level security';
        execute 'alter table "MedicalRecords" force row level security';
    end if;
    if exists (select 1 from information_schema.tables where table_name='Invoices') then
        execute 'alter table "Invoices" enable row level security';
        execute 'alter table "Invoices" force row level security';
    end if;
    if exists (select 1 from information_schema.tables where table_name='InsuranceClaims') then
        execute 'alter table "InsuranceClaims" enable row level security';
        execute 'alter table "InsuranceClaims" force row level security';
    end if;
end $$;

-- Helper function to read app role / patient context without error if unset
create or replace function app_context(role_key text) returns text as $$
begin
    return current_setting(role_key, true); -- returns null if not set
end; $$ language plpgsql stable;

-- Broad policies (permissive) - replace later with restrictive ones
do $$ begin
    -- Patients policies
    if not exists (select 1 from pg_policies where tablename='Patients' and policyname='patients_allow_all_admin') then
        execute $$create policy patients_allow_all_admin on "Patients"
            for all using (
                coalesce(app_context('app.role'),'') in ('admin','clinician','billing')
                or coalesce(app_context('app.patient_id'),'')::text = "Id"::text
            )$$;
    end if;
    -- MedicalRecords policy
    if not exists (select 1 from pg_policies where tablename='MedicalRecords' and policyname='medicalrecords_allow_admin_clinician') then
        execute $$create policy medicalrecords_allow_admin_clinician on "MedicalRecords"
            for all using (
                coalesce(app_context('app.role'),'') in ('admin','clinician')
                or (coalesce(app_context('app.patient_id'),'')::text = "PatientId"::text and "Sensitivity" <= 3)
            )$$;
    end if;
    -- Invoices policy
    if not exists (select 1 from pg_policies where tablename='Invoices' and policyname='invoices_allow_admin_billing_patient') then
        execute $$create policy invoices_allow_admin_billing_patient on "Invoices"
            for all using (
                coalesce(app_context('app.role'),'') in ('admin','billing')
                or coalesce(app_context('app.patient_id'),'')::text = "PatientId"::text
            )$$;
    end if;
    -- InsuranceClaims policy
    if not exists (select 1 from pg_policies where tablename='InsuranceClaims' and policyname='claims_allow_admin_billing') then
        execute $$create policy claims_allow_admin_billing on "InsuranceClaims"
            for all using (
                coalesce(app_context('app.role'),'') in ('admin','billing','clinician')
            )$$;
    end if;
end $$;

-- B6. Security view summarizing classification coverage
create or replace view "PHI_FieldCoverage" as
    select fc."TableName", fc."ColumnName", fc."Classification", fc."Sensitivity",
           (select count(*) from information_schema.columns c where c.table_name = fc."TableName") as total_columns_in_table
    from "FieldClassifications" fc
    where fc."Classification" in ('phi','pii','secret')
    order by fc."TableName", fc."Sensitivity" desc;

-- B7. Comment markers for future tightening
comment on view "PHI_FieldCoverage" is 'Lists classified sensitive fields to audit coverage and prioritize RLS masking.';
comment on table "EncryptionKeys" is 'Tracks encryption key lifecycle; referenced by MedicalRecords.KeyVersion.';
comment on table "FieldClassifications" is 'Data classification registry for governance & masking.';

-- Patch Set B complete.

-- =====================================================
-- PATCH SET C: WORKFLOW, SCHEDULING & OPERATIONAL REFINEMENTS
-- =====================================================

-- C1. Provider Licensing & Credential Documents
create table if not exists "ProviderLicenses" (
    "Id"            bigserial primary key,
    "ProviderId"    integer not null references "Providers" on delete cascade,
    "LicenseType"   text not null, -- RN, MD, NP, etc.
    "LicenseNumber" text not null,
    "Jurisdiction"  text, -- State/Province code
    "IssuedAt"      date,
    "ExpiresAt"     date,
    "Status"        text, -- active, expired, suspended, revoked
    "Metadata"      jsonb,
    "CreatedAt"     timestamptz default now(),
    "UpdatedAt"     timestamptz default now(),
    unique ("ProviderId","LicenseType","LicenseNumber","Jurisdiction")
);

create index if not exists "IX_ProviderLicenses_Expiry" on "ProviderLicenses" ("ExpiresAt") where "ExpiresAt" is not null;

create table if not exists "ProviderLicenseHistory" (
    "Id"            bigserial primary key,
    "ProviderLicenseId" bigint not null references "ProviderLicenses" on delete cascade,
    "OldStatus"     text,
    "NewStatus"     text not null,
    "ChangedAt"     timestamptz default now(),
    "ChangedBy"     text,
    "Reason"        text
);

-- C2. Availability Exceptions (blackouts, overrides)
create table if not exists "AvailabilityExceptions" (
    "Id"           bigserial primary key,
    "ProviderId"   integer not null references "Providers" on delete cascade,
    "StartsAt"     timestamptz not null,
    "EndsAt"       timestamptz not null,
    "Type"         text not null, -- blackout, override
    "Reason"       text,
    "CreatedAt"    timestamptz default now(),
    "UpdatedAt"    timestamptz default now(),
    constraint "CK_AvailabilityExceptions_TimeOrder" check ("EndsAt" > "StartsAt")
);
create index if not exists "IX_AvailabilityExceptions_Provider_Time" on "AvailabilityExceptions" ("ProviderId","StartsAt","EndsAt");

-- C3. Generic Workflow Instances & History for extensible state machines
create table if not exists "WorkflowInstances" (
    "Id"             bigserial primary key,
    "WorkflowDefinitionId" integer not null references "WorkflowDefinitions" on delete restrict,
    "EntityType"     text not null,
    "EntityId"       bigint not null,
    "CurrentState"   text not null,
    "StartedAt"      timestamptz default now(),
    "CompletedAt"    timestamptz,
    "Metadata"       jsonb,
    unique ("WorkflowDefinitionId","EntityType","EntityId")
);

create table if not exists "WorkflowStateHistory" (
    "Id"             bigserial primary key,
    "WorkflowInstanceId" bigint not null references "WorkflowInstances" on delete cascade,
    "FromState"      text,
    "ToState"        text not null,
    "ChangedAt"      timestamptz default now(),
    "ChangedBy"      text,
    "Reason"         text
);
create index if not exists "IX_WorkflowStateHistory_Instance" on "WorkflowStateHistory" ("WorkflowInstanceId","ChangedAt" desc);

-- C4. Notification Delivery Attempts
create table if not exists "NotificationAttempts" (
    "Id"            bigserial primary key,
    "NotificationId" bigint not null references "NotificationQueue" on delete cascade,
    "AttemptNumber" integer not null,
    "Status"        notification_status not null,
    "ProviderResponse" jsonb,
    "Error"         text,
    "DurationMs"    integer,
    "CreatedAt"     timestamptz default now(),
    unique ("NotificationId","AttemptNumber")
);
create index if not exists "IX_NotificationAttempts_Notification" on "NotificationAttempts" ("NotificationId");

-- C5. Strengthen ProviderTimeSlots appointment binding and overlap safety enhancements
create unique index if not exists "UQ_ProviderTimeSlots_Appointment" on "ProviderTimeSlots" ("AppointmentId") where "AppointmentId" is not null;

-- C6. Function to atomically reserve a time slot for an appointment
create or replace function reserve_time_slot(
    p_provider_id integer,
    p_start timestamptz,
    p_end timestamptz,
    p_service_type_id integer,
    p_appointment_id integer
) returns bigint as $$
declare v_slot_id bigint; begin
    -- Try to find existing available matching slot (exact match first)
    select "Id" into v_slot_id from "ProviderTimeSlots"
      where "ProviderId"=p_provider_id and "StartTime"=p_start and "EndTime"=p_end
        and ("ServiceTypeId" = p_service_type_id or "ServiceTypeId" is null)
        and "Status"='available' and "DeletedAt" is null
      for update skip locked limit 1;
    if v_slot_id is null then
        -- Create ad-hoc slot if none exists (optional strategy)
        insert into "ProviderTimeSlots" ("ProviderId","ServiceTypeId","StartTime","EndTime","Status")
        values (p_provider_id, p_service_type_id, p_start, p_end, 'available') returning "Id" into v_slot_id;
    end if;
    update "ProviderTimeSlots" set "Status"='booked', "AppointmentId"=p_appointment_id, "UpdatedAt"=now()
      where "Id"=v_slot_id and "Status"='available';
    if not found then
        raise exception 'Failed to reserve slot (already booked)';
    end if;
    return v_slot_id;
end; $$ language plpgsql;

comment on function reserve_time_slot(integer,timestamptz,timestamptz,integer,integer) is 'Atomically reserves (or creates) a provider time slot for an appointment.';

-- C7. Invoice paid amount maintenance trigger from PaymentTransactions
create or replace function reconcile_invoice_paid_amount(p_invoice_id integer) returns void as $$
begin
    update "Invoices" i
       set "PaidAmount" = coalesce((select sum(case when "Type"='charge' and "Status" in ('captured','settled') then "Amount" when "Type"='refund' and "Status" in ('settled','refunded') then -"Amount" else 0 end)
                                      from "PaymentTransactions" pt where pt."InvoiceId"=i."Id"),0),
           "PaidAt" = case when (select coalesce(sum(case when "Type"='charge' and "Status" in ('captured','settled') then "Amount" when "Type"='refund' and "Status" in ('settled','refunded') then -"Amount" else 0 end),0) from "PaymentTransactions" pt where pt."InvoiceId"=i."Id") >= i."Total" and i."Total" > 0 then now() else i."PaidAt" end
     where i."Id" = p_invoice_id;
end; $$ language plpgsql;

create or replace function trg_paymenttransactions_invoice_update() returns trigger as $$
begin
    perform reconcile_invoice_paid_amount(NEW."InvoiceId");
    return NEW;
end; $$ language plpgsql;

drop trigger if exists paymenttransactions_invoice_update on "PaymentTransactions";
create trigger paymenttransactions_invoice_update after insert or update of "Status","Amount","Type" on "PaymentTransactions"
    for each row when (NEW."InvoiceId" is not null) execute function trg_paymenttransactions_invoice_update();

-- C8. Auto workflow instance creation for appointments (simple optional scaffolding)
create or replace function trg_appointments_workflow_instance() returns trigger as $$
declare wf_def_id integer; inst_id bigint; begin
    -- Attempt to find default workflow for appointments
    select "Id" into wf_def_id from "WorkflowDefinitions" where "EntityType"='appointment' and "IsDefault"=true limit 1;
    if wf_def_id is not null then
        begin
            insert into "WorkflowInstances" ("WorkflowDefinitionId","EntityType","EntityId","CurrentState")
            values (wf_def_id,'appointment',NEW."Id", NEW."Status") returning "Id" into inst_id;
            insert into "WorkflowStateHistory" ("WorkflowInstanceId","FromState","ToState") values (inst_id,null, NEW."Status");
        exception when unique_violation then
            -- already exists; ignore
            null;
        end;
    end if;
    return NEW;
end; $$ language plpgsql;

drop trigger if exists appointments_workflow_instance on "Appointments";
create trigger appointments_workflow_instance after insert on "Appointments" for each row execute function trg_appointments_workflow_instance();

-- C9. On appointment status change also update workflow instance state
create or replace function trg_appointments_workflow_state_update() returns trigger as $$
declare inst_id bigint; begin
    select "Id" into inst_id from "WorkflowInstances" where "EntityType"='appointment' and "EntityId"=NEW."Id" limit 1;
    if inst_id is not null and NEW."Status" is distinct from OLD."Status" then
        update "WorkflowInstances" set "CurrentState"=NEW."Status", "CompletedAt" = case when NEW."Status" in ('completed','cancelled') then coalesce("CompletedAt", now()) else "CompletedAt" end
          where "Id"=inst_id;
        insert into "WorkflowStateHistory" ("WorkflowInstanceId","FromState","ToState") values (inst_id, OLD."Status", NEW."Status");
    end if;
    return NEW;
end; $$ language plpgsql;

drop trigger if exists appointments_workflow_state_update on "Appointments";
create trigger appointments_workflow_state_update after update of "Status" on "Appointments" for each row execute function trg_appointments_workflow_state_update();

-- C10. Comments noting deprecated columns scheduled removal timeline
comment on column "Providers"."ServicesOffered" is 'DEPRECATED: replace with ProviderServiceTypes; plan removal after data migration.';
comment on column "Providers"."AvailabilitySchedule" is 'DEPRECATED: replace with ScheduleTemplates + ProviderTimeSlots; plan removal after migration.';

-- Patch Set C complete.


-- =====================================================
-- PATCH SET D: CODING, CLAIM DETAIL, RLS TIGHTENING & MASKING
-- =====================================================
-- Goals:
--  D1. Introduce standardized medical code infrastructure (ICD / CPT / HCPCS, etc.).
--  D2. Add claim line items referencing individual codes & amounts to enrich adjudication tracking.
--  D3. Refine Row Level Security (replace broad permissive policies with granular role/state aware policies).
--  D4. Add masking view(s) exposing only minimally necessary patient fields for general operational contexts.
--  D5. Helper functions for session context & sensitivity authorization.
--  D6. Deprecation staging for legacy JSON columns (not dropped yet; explicit migration required before removal).

-- D1. Code Systems & Codes -----------------------------------------------------
create table if not exists "CodeSystems" (
    "Id"            serial primary key,
    "Name"          text not null unique,       -- e.g. ICD-10-CM, CPT, HCPCS
    "Version"       text,
    "Description"   text,
    "Authority"     text,                       -- WHO, AMA, CMS
    "EffectiveFrom" date,
    "EffectiveTo"   date,
    "CreatedAt"     timestamptz default now(),
    "UpdatedAt"     timestamptz default now()
);

create table if not exists "MedicalCodes" (
    "Id"            bigserial primary key,
    "CodeSystemId"  integer not null references "CodeSystems" on delete cascade,
    "Code"          text not null,
    "ShortDescription" text,
    "LongDescription"  text,
    "IsBillable"    boolean,
    "EffectiveFrom" date,
    "EffectiveTo"   date,
    "Metadata"      jsonb,
    "CreatedAt"     timestamptz default now(),
    "UpdatedAt"     timestamptz default now(),
    unique ("CodeSystemId","Code")
);

create index if not exists "IX_MedicalCodes_CodeSystem_Code" on "MedicalCodes" ("CodeSystemId","Code");
create index if not exists "IX_MedicalCodes_CodeSystem_Billable" on "MedicalCodes" ("CodeSystemId") where "IsBillable" = true;

-- UpdatedAt triggers for new tables
do $$ begin
    if exists (select 1 from pg_proc where proname='set_updated_at') then
        execute 'drop trigger if exists trg_codesystems_updated on "CodeSystems"';
        execute 'create trigger trg_codesystems_updated before update on "CodeSystems" for each row execute function set_updated_at()';
        execute 'drop trigger if exists trg_medicalcodes_updated on "MedicalCodes"';
        execute 'create trigger trg_medicalcodes_updated before update on "MedicalCodes" for each row execute function set_updated_at()';
    end if;
end $$;

-- D2. Claim Line Items ---------------------------------------------------------
create table if not exists "ClaimLineItems" (
    "Id"              bigserial primary key,
    "ClaimId"         integer not null references "InsuranceClaims" on delete cascade,
    "CodeId"          bigint references "MedicalCodes" on delete set null,
    "CodeSystemId"    integer references "CodeSystems" on delete set null, -- redundancy for faster filtering
    "Code"            text,   -- denormalized code text (for historical immutability even if code table changes)
    "Description"     text,
    "Quantity"        numeric(10,2) default 1,
    "UnitCharge"      numeric(12,2) default 0,
    "LineCharge"      numeric(12,2) generated always as (coalesce("Quantity",0) * coalesce("UnitCharge",0)) stored,
    "ApprovedAmount"  numeric(12,2),
    "DeniedAmount"    numeric(12,2),
    "Status"          claim_status default 'pending',
    "Metadata"        jsonb,
    "CreatedAt"       timestamptz default now(),
    "UpdatedAt"       timestamptz default now()
);

create index if not exists "IX_ClaimLineItems_Claim" on "ClaimLineItems" ("ClaimId");
create index if not exists "IX_ClaimLineItems_CodeSystem_Code" on "ClaimLineItems" ("CodeSystemId","Code");

do $$ begin
    if exists (select 1 from pg_proc where proname='set_updated_at') then
        execute 'drop trigger if exists trg_claimlineitems_updated on "ClaimLineItems"';
        execute 'create trigger trg_claimlineitems_updated before update on "ClaimLineItems" for each row execute function set_updated_at()';
    end if;
end $$;

-- Enrich InsuranceClaims with financial rollups (if not already present)
alter table "InsuranceClaims" add column if not exists "BilledAmount" numeric(12,2);
alter table "InsuranceClaims" add column if not exists "PaidAmount" numeric(12,2);

-- Rolling up PaidAmount / ApprovedAmount (simple helper; can be invoked post-adjudication)
create or replace function reconcile_claim_amounts(p_claim_id integer) returns void as $$
begin
    update "InsuranceClaims" c
       set "ApprovedAmount" = coalesce((select sum(coalesce("ApprovedAmount",0)) from "ClaimLineItems" cli where cli."ClaimId"=c."Id"), c."ApprovedAmount"),
           "PaidAmount" = coalesce((select sum(case when cli."Status" in ('paid','partial') then coalesce(cli."ApprovedAmount",0) else 0 end) from "ClaimLineItems" cli where cli."ClaimId"=c."Id"), c."PaidAmount")
     where c."Id" = p_claim_id;
end; $$ language plpgsql;

comment on function reconcile_claim_amounts(integer) is 'Recalculates InsuranceClaims Approved/Paid amounts from ClaimLineItems.';

-- Trigger to auto-reconcile on line item changes
create or replace function trg_claimlineitems_claim_recalc() returns trigger as $$
begin
    perform reconcile_claim_amounts(NEW."ClaimId");
    return NEW;
end; $$ language plpgsql;

drop trigger if exists claimlineitems_claim_recalc on "ClaimLineItems";
create trigger claimlineitems_claim_recalc after insert or update of "ApprovedAmount","Status","Quantity","UnitCharge" on "ClaimLineItems"
    for each row execute function trg_claimlineitems_claim_recalc();

-- D3. Refined RLS Policies -----------------------------------------------------
-- Strategy: Drop broad permissive policies created in Patch B and replace with scoped policies.
-- Assumptions: application sets custom GUCs app.role, app.patient_id.

do $$ begin
    -- Drop existing broad policies if they exist
    if exists (select 1 from pg_policies where tablename='Patients' and policyname='patients_allow_all_admin') then
        execute 'drop policy patients_allow_all_admin on "Patients"';
    end if;
    if exists (select 1 from pg_policies where tablename='MedicalRecords' and policyname='medicalrecords_allow_admin_clinician') then
        execute 'drop policy medicalrecords_allow_admin_clinician on "MedicalRecords"';
    end if;
    if exists (select 1 from pg_policies where tablename='Invoices' and policyname='invoices_allow_admin_billing_patient') then
        execute 'drop policy invoices_allow_admin_billing_patient on "Invoices"';
    end if;
    if exists (select 1 from pg_policies where tablename='InsuranceClaims' and policyname='claims_allow_admin_billing') then
        execute 'drop policy claims_allow_admin_billing on "InsuranceClaims"';
    end if;
end $$;

-- Helper: sensitivity / classification gate
create or replace function classification_allowed(p_sensitivity int) returns boolean as $$
declare v_role text := coalesce(app_context('app.role'),''); begin
    -- Role-based maximum sensitivity (simple heuristic; adjust as needed)
    if v_role = 'admin' then return true; end if;
    if v_role = 'clinician' then return p_sensitivity <= 9; end if;
    if v_role = 'billing' then return p_sensitivity <= 3; end if;
    if v_role = 'patient' then return p_sensitivity <= 3; end if;
    -- default least privilege
    return p_sensitivity <= 1;
end; $$ language plpgsql stable;

comment on function classification_allowed(int) is 'Evaluates whether current session role (app.role) can access field with given sensitivity.';

-- Patients policies
create policy patients_select on "Patients"
    for select using (
        coalesce(app_context('app.role'),'') in ('admin','clinician','billing')
        or coalesce(app_context('app.patient_id'),'')::text = "Id"::text
    );
create policy patients_modify on "Patients"
    for update using (coalesce(app_context('app.role'),'') in ('admin','clinician'));
create policy patients_insert on "Patients"
    for insert with check (coalesce(app_context('app.role'),'') in ('admin','clinician'));
create policy patients_delete on "Patients"
    for delete using (coalesce(app_context('app.role'),'') = 'admin');

-- MedicalRecords policies (sensitivity aware)
create policy medicalrecords_select on "MedicalRecords"
    for select using (
        coalesce(app_context('app.role'),'') in ('admin','clinician')
        or (coalesce(app_context('app.role'),'') = 'patient' and coalesce(app_context('app.patient_id'),'')::int = "PatientId" and classification_allowed("Sensitivity"))
    );
create policy medicalrecords_modify on "MedicalRecords"
    for update using (coalesce(app_context('app.role'),'') in ('admin','clinician'));
create policy medicalrecords_insert on "MedicalRecords"
    for insert with check (coalesce(app_context('app.role'),'') in ('admin','clinician'));
create policy medicalrecords_delete on "MedicalRecords"
    for delete using (coalesce(app_context('app.role'),'') = 'admin');

-- Invoices policies
create policy invoices_select on "Invoices"
    for select using (
        coalesce(app_context('app.role'),'') in ('admin','billing')
        or coalesce(app_context('app.patient_id'),'')::int = "PatientId"
    );
create policy invoices_modify on "Invoices"
    for update using (coalesce(app_context('app.role'),'') in ('admin','billing'));
create policy invoices_insert on "Invoices"
    for insert with check (coalesce(app_context('app.role'),'') in ('admin','billing'));
create policy invoices_delete on "Invoices"
    for delete using (coalesce(app_context('app.role'),'') = 'admin');

-- InsuranceClaims policies
create policy claims_select on "InsuranceClaims"
    for select using (coalesce(app_context('app.role'),'') in ('admin','billing','clinician'));
create policy claims_modify on "InsuranceClaims"
    for update using (coalesce(app_context('app.role'),'') in ('admin','billing'));
create policy claims_insert on "InsuranceClaims"
    for insert with check (coalesce(app_context('app.role'),'') in ('admin','billing'));
create policy claims_delete on "InsuranceClaims"
    for delete using (coalesce(app_context('app.role'),'') = 'admin');

-- ClaimLineItems policies (inherit InsuranceClaims authority)
alter table "ClaimLineItems" enable row level security;
alter table "ClaimLineItems" force row level security;
create policy claimlineitems_select on "ClaimLineItems"
    for select using (coalesce(app_context('app.role'),'') in ('admin','billing','clinician'));
create policy claimlineitems_modify on "ClaimLineItems"
    for update using (coalesce(app_context('app.role'),'') in ('admin','billing'));
create policy claimlineitems_insert on "ClaimLineItems"
    for insert with check (coalesce(app_context('app.role'),'') in ('admin','billing'));
create policy claimlineitems_delete on "ClaimLineItems"
    for delete using (coalesce(app_context('app.role'),'') = 'admin');

-- D4. Masking / minimal exposure view (exclude high sensitivity & PII fields as needed)
create or replace view "PatientsProtected" as
    select "Id",
           case when coalesce(app_context('app.role'),'') in ('admin','clinician') then "FirstName" else left("FirstName",1) || '.' end as "FirstName",
           case when coalesce(app_context('app.role'),'') in ('admin','clinician') then "LastName" else left("LastName",1) || '.' end as "LastName",
           "DateOfBirth" -- Could be age only; retaining DOB for clinicians & admins; for others could transform further.
             filter (where coalesce(app_context('app.role'),'') in ('admin','clinician')) as "DateOfBirth",
           case when coalesce(app_context('app.role'),'') in ('admin','clinician','billing') then "MedicalRecordNumber" else null end as "MedicalRecordNumber",
           "Gender",
           case when coalesce(app_context('app.role'),'') in ('admin','clinician','billing') then "Email" else null end as "Email",
           case when coalesce(app_context('app.role'),'') in ('admin','clinician','billing') then "Phone" else null end as "Phone",
           "IsActive",
           "CreatedAt",
           "UpdatedAt"
    from "Patients"
    where "DeletedAt" is null;

comment on view "PatientsProtected" is 'Role-aware masked patient projection (limits PII visibility for non-privileged roles).';

-- D5. Session utility to set app context (wrapper around set_config for convenience)
create or replace function set_app_context(p_role text default null, p_patient_id int default null) returns void as $$
begin
    if p_role is not null then
        perform set_config('app.role', p_role, true);
    end if;
    if p_patient_id is not null then
        perform set_config('app.patient_id', p_patient_id::text, true);
    end if;
end; $$ language plpgsql security definer;

comment on function set_app_context(text,int) is 'Sets session GUCs (app.role, app.patient_id) for RLS evaluation.';

-- D6. Deprecation staging (comments) -------------------------------------------
comment on column "Patients"."EmergencyContact" is 'DEPRECATED: replaced by PatientContacts; schedule drop after data validation.';
comment on column "Patients"."Address" is 'DEPRECATED: replaced by Addresses/PatientAddresses; schedule drop after migration.';
comment on column "Patients"."InsuranceInfo" is 'DEPRECATED: replaced by PatientInsurancePolicies; schedule drop after migration.';

-- Suggested future migration script (not executed here):
-- 1. Verify no active references to deprecated JSON columns.
-- 2. Create backup table capturing old JSON for audit.
-- 3. Alter table Patients drop column ... (repeat for each) inside a controlled release.

-- Patch Set D complete.


-- =====================================================
-- PATCH SET E: ENCRYPTION LIFECYCLE, PARTITIONING HELPERS, INBOUND EVENTS
-- =====================================================
-- Goals:
--  E1. Automate key lifecycle (activation, retirement) & default key assignment for new MedicalRecords.
--  E2. Track re-encryption backlog and provide functions to mark completion.
--  E3. Partitioning scaffolding helpers (month-based) without destructive migration; optional parallel partitioned tables.
--  E4. Inbound event ingestion (for external system messages) with deduplication support.

-- E1. Encryption Key Lifecycle Functions ---------------------------------------
create or replace function encryption_activate_key(p_key_version int) returns boolean as $$
declare v_exists boolean; begin
    select true into v_exists from "EncryptionKeys" where "KeyVersion" = p_key_version;
    if not v_exists then raise exception 'Key version % not found', p_key_version; end if;
    -- Set all active to retiring (except compromised/retired)
    update "EncryptionKeys" set "Status"='retiring', "RetiredAt"=coalesce("RetiredAt", now())
      where "Status"='active' and "KeyVersion" <> p_key_version;
    -- Activate target
    update "EncryptionKeys" set "Status"='active', "ActivatedAt"=coalesce("ActivatedAt", now()) where "KeyVersion" = p_key_version;
    return true;
end; $$ language plpgsql security definer;

comment on function encryption_activate_key(int) is 'Marks specified key active; previous active keys set to retiring.';

create or replace function encryption_retire_key(p_key_version int) returns boolean as $$
begin
    update "EncryptionKeys" set "Status"='retired', "RetiredAt"=coalesce("RetiredAt", now())
      where "KeyVersion" = p_key_version and "Status" in ('active','retiring');
    return found;
end; $$ language plpgsql security definer;

comment on function encryption_retire_key(int) is 'Retires key (active/retiring -> retired).';

create or replace function encryption_current_key() returns int as $$
declare v_k int; begin
    select "KeyVersion" into v_k from "EncryptionKeys" where "Status"='active' order by "ActivatedAt" desc nulls last limit 1;
    return v_k;
end; $$ language plpgsql stable;

comment on function encryption_current_key() is 'Returns current active encryption key version (highest precedence).';

-- Assign default key to new medical records if not provided
create or replace function trg_medicalrecords_assign_key() returns trigger as $$
declare v_key int; begin
    if NEW."KeyVersion" is null then
        v_key := encryption_current_key();
        if v_key is null then
            -- Auto-create initial key if none exists
            insert into "EncryptionKeys" ("KMSAlias","Status","ActivatedAt") values ('primary', 'active', now()) returning "KeyVersion" into v_key;
        end if;
        NEW."KeyVersion" := v_key;
    end if;
    return NEW;
end; $$ language plpgsql;

drop trigger if exists medicalrecords_assign_key on "MedicalRecords";
create trigger medicalrecords_assign_key before insert on "MedicalRecords" for each row execute function trg_medicalrecords_assign_key();

-- E2. Re-encryption Backlog Tracking -------------------------------------------
alter table "MedicalRecords" add column if not exists "ReencryptedAt" timestamptz;

create or replace view "MedicalRecords_Reencrypt_Backlog" as
    select mr.*,
           ek."Status" as key_status,
           (select encryption_current_key()) as active_key
    from "MedicalRecords" mr
    left join "EncryptionKeys" ek on ek."KeyVersion" = mr."KeyVersion"
    where mr."DeletedAt" is null
      and mr."KeyVersion" <> (select encryption_current_key())
      and (ek."Status" in ('retiring','retired','compromised') or mr."ReencryptedAt" is null);

comment on view "MedicalRecords_Reencrypt_Backlog" is 'Lists medical records needing re-encryption to current active key.';

create or replace function mark_record_reencrypted(p_record_id int, p_new_key_version int) returns boolean as $$
begin
    update "MedicalRecords" set "KeyVersion" = p_new_key_version, "ReencryptedAt" = now(), "UpdatedAt" = now()
     where "Id" = p_record_id;
    return found;
end; $$ language plpgsql security definer;

comment on function mark_record_reencrypted(int,int) is 'Updates a medical record to reflect re-encryption with new key version.';

-- E3. Partitioning Helper Infrastructure ---------------------------------------
-- Non-intrusive: create helper functions; actual conversion of existing base tables left to migration procedure.

create or replace function ensure_month_partition(p_parent regclass, p_ts_column text, p_for_date date) returns text as $$
declare v_start date := date_trunc('month', p_for_date)::date;
        v_end   date := (date_trunc('month', p_for_date) + interval '1 month')::date;
        v_part_name text;
        v_parent_name text := (select relname from pg_class where oid = p_parent);
        v_full_name text;
begin
    v_part_name := format('%s_%s', v_parent_name, to_char(v_start,'YYYYMM'));
    v_full_name := quote_ident(v_part_name);
    if not exists (select 1 from pg_class where relname = v_part_name) then
        execute format('create table if not exists %s partition of %s for values from (%L) to (%L)',
                       v_full_name, v_parent_name, v_start, v_end);
    end if;
    return v_part_name;
end; $$ language plpgsql;

comment on function ensure_month_partition(regclass,text,date) is 'Ensures (or creates) a monthly partition for a partitioned parent table.';

-- Optional: create parallel partitioned audit table if starting fresh (no data migration executed automatically)
do $$ begin
    if not exists (select 1 from information_schema.tables where table_name='AuditEventsPart') then
        execute 'create table "AuditEventsPart" (like "AuditEvents" including all) partition by range ("Timestamp")';
    end if;
end $$;

-- Seed current + next month partitions (idempotent)
do $$ begin
    if exists (select 1 from information_schema.tables where table_name='AuditEventsPart') then
        perform ensure_month_partition('AuditEventsPart'::regclass,'Timestamp', current_date);
        perform ensure_month_partition('AuditEventsPart'::regclass,'Timestamp', current_date + interval '1 month');
    end if;
end $$;

-- Routing trigger (only if using partitioned variant; disabled by default comment)
-- Uncomment to enable routing then insert into AuditEventsPart instead of AuditEvents:
-- create or replace function trg_auditevents_part_route() returns trigger as $$
-- begin
--     perform ensure_month_partition('AuditEventsPart'::regclass,'Timestamp', NEW."Timestamp"::date);
--     return NEW;
-- end; $$ language plpgsql;
-- drop trigger if exists auditevents_part_route on "AuditEventsPart";
-- create trigger auditevents_part_route before insert on "AuditEventsPart" for each row execute function trg_auditevents_part_route();

-- E4. Inbound Event Ingestion --------------------------------------------------
do $$ begin
    if not exists (select 1 from pg_type where typname='inbound_event_status') then
        create type inbound_event_status as enum ('received','validated','processed','failed','ignored');
    end if;
end $$;

create table if not exists "InboundEvents" (
    "Id"             bigserial primary key,
    "SourceSystem"   text not null,              -- ehr, lab, etc.
    "EventType"      text not null,              -- patient.update, result.new
    "ExternalId"     text,                       -- external event id
    "DedupKey"       text,                       -- hash or composite key to deduplicate
    "Status"         inbound_event_status not null default 'received',
    "Payload"        jsonb not null,
    "ReceivedAt"     timestamptz default now(),
    "ProcessedAt"    timestamptz,
    "Error"          text,
    "CorrelationId"  uuid,
    "Metadata"       jsonb,
    "CreatedAt"      timestamptz default now(),
    "UpdatedAt"      timestamptz default now(),
    unique ("SourceSystem","DedupKey")
);

create index if not exists "IX_InboundEvents_Status_ReceivedAt" on "InboundEvents" ("Status","ReceivedAt");
create index if not exists "IX_InboundEvents_Correlation" on "InboundEvents" ("CorrelationId");

drop trigger if exists trg_inboundevents_updated on "InboundEvents";
create trigger trg_inboundevents_updated before update on "InboundEvents" for each row execute function set_updated_at();

-- Suggested retention seed (90 days)
insert into "DataRetentionPolicies" ("TableName","RetainDays") values ('InboundEvents', 90)
    on conflict ("TableName") do nothing;

comment on table "InboundEvents" is 'Inbound external integration events (idempotent ingestion with DedupKey).';

-- Patch Set E complete.

-- =====================================================
-- PATCH SET F: MASKING, AUDIT TRIGGERS, DUAL-WRITE, RE-ENCRYPTION BATCHES
-- =====================================================
-- Goals:
--  F1. Centralized masking helper functions (names, email, phone, MRN) & refreshed PatientsProtected view.
--  F2. Fine-grained audit triggers on Patients & MedicalRecords (insert/update/delete) using log_audit_event.
--  F3. Dual-write trigger from AuditEvents -> AuditEventsPart (opt-in via GUC app.audit_dual_write='on').
--  F4. Re-encryption batch scheduling & tracking tables + functions.

-- F1. Masking Helper Functions -------------------------------------------------
create or replace function mask_name(p_name text) returns text as $$
begin
    if p_name is null then return null; end if;
    if length(p_name) <= 1 then return p_name || '.'; end if;
    return left(p_name,1) || repeat('*', greatest(0, length(p_name)-2)) || right(p_name,1);
end; $$ language plpgsql immutable;

comment on function mask_name(text) is 'Masks a name retaining first & last character.';

create or replace function mask_email(p_email text) returns text as $$
declare l_at int; local_part text; domain text; begin
    if p_email is null then return null; end if;
    l_at := position('@' in p_email);
    if l_at = 0 then return mask_name(p_email); end if;
    local_part := left(p_email, l_at-1);
    domain := substring(p_email from l_at+1);
    if length(local_part) <= 2 then
        local_part := left(local_part,1) || '*';
    else
        local_part := left(local_part,1) || repeat('*', length(local_part)-2) || right(local_part,1);
    end if;
    return local_part || '@' || domain;
end; $$ language plpgsql immutable;

comment on function mask_email(text) is 'Masks an email local part preserving first/last char.';

create or replace function mask_phone(p_phone text) returns text as $$
begin
    if p_phone is null then return null; end if;
    -- keep last 2 digits
    return '***-***-' || right(regexp_replace(p_phone,'[^0-9]','','g'),2);
end; $$ language plpgsql immutable;

comment on function mask_phone(text) is 'Masks phone number keeping only last two digits.';

create or replace function mask_mrn(p_mrn text) returns text as $$
begin
    if p_mrn is null then return null; end if;
    return left(p_mrn,2) || repeat('*', greatest(0,length(p_mrn)-4)) || right(p_mrn,2);
end; $$ language plpgsql immutable;

comment on function mask_mrn(text) is 'Masks medical record number retaining first 2 & last 2 chars.';

-- Refresh PatientsProtected view to leverage masking helpers & simplify logic
drop view if exists "PatientsProtected";
create view "PatientsProtected" as
    select "Id",
           case when coalesce(app_context('app.role'),'') in ('admin','clinician') then "FirstName" else mask_name("FirstName") end as "FirstName",
           case when coalesce(app_context('app.role'),'') in ('admin','clinician') then "LastName" else mask_name("LastName") end as "LastName",
           -- For non-clinician/admin, expose only approximate age (years) instead of DOB
           case when coalesce(app_context('app.role'),'') in ('admin','clinician') then "DateOfBirth" else date_trunc('year', age("DateOfBirth")) end as "DateOfBirth",
           case when coalesce(app_context('app.role'),'') in ('admin','clinician','billing') then "MedicalRecordNumber" else mask_mrn("MedicalRecordNumber") end as "MedicalRecordNumber",
           "Gender",
           case when coalesce(app_context('app.role'),'') in ('admin','clinician','billing') then "Email" else mask_email("Email") end as "Email",
           case when coalesce(app_context('app.role'),'') in ('admin','clinician','billing') then "Phone" else mask_phone("Phone") end as "Phone",
           "IsActive","CreatedAt","UpdatedAt"
    from "Patients" where "DeletedAt" is null;

comment on view "PatientsProtected" is 'Role-aware masked patient projection; non-privileged roles receive masked PII.';

-- F2. Audit Triggers ------------------------------------------------------------
-- Unified trigger function for Patients
create or replace function trg_patients_audit() returns trigger as $$
declare v_user text := coalesce(app_context('app.current_user_id'), current_setting('app.current_user_id', true));
        v_role text := coalesce(app_context('app.role'), current_setting('app.role', true));
        v_meta jsonb := '{}'::jsonb; begin
    if TG_OP = 'INSERT' then
        v_meta := jsonb_build_object('new', to_jsonb(NEW));
        perform log_audit_event('patient', NEW."Id", 'insert', v_user, 'success', null, v_role, null, v_meta);
        return NEW;
    elsif TG_OP = 'UPDATE' then
        v_meta := jsonb_build_object('old', to_jsonb(OLD),'new', to_jsonb(NEW));
        perform log_audit_event('patient', NEW."Id", 'update', v_user, 'success', null, v_role, null, v_meta);
        return NEW;
    elsif TG_OP = 'DELETE' then
        v_meta := jsonb_build_object('old', to_jsonb(OLD));
        perform log_audit_event('patient', OLD."Id", 'delete', v_user, 'success', null, v_role, null, v_meta);
        return OLD;
    end if;
    return null;
end; $$ language plpgsql security definer;

drop trigger if exists patients_audit_ins on "Patients";
drop trigger if exists patients_audit_upd on "Patients";
drop trigger if exists patients_audit_del on "Patients";
create trigger patients_audit_ins after insert on "Patients" for each row execute function trg_patients_audit();
create trigger patients_audit_upd after update on "Patients" for each row execute function trg_patients_audit();
create trigger patients_audit_del after delete on "Patients" for each row execute function trg_patients_audit();

-- MedicalRecords audit trigger (excluding potentially large Content; include hash & metadata)
create or replace function trg_medicalrecords_audit() returns trigger as $$
declare v_user text := coalesce(app_context('app.current_user_id'), current_setting('app.current_user_id', true));
        v_role text := coalesce(app_context('app.role'), current_setting('app.role', true));
        v_meta jsonb := '{}'::jsonb; begin
    if TG_OP = 'INSERT' then
        v_meta := jsonb_build_object('patient_id', NEW."PatientId", 'record_type', NEW."RecordType", 'key_version', NEW."KeyVersion", 'sensitivity', NEW."Sensitivity", 'content_hash', NEW."ContentHash");
        perform log_audit_event('medical_record', NEW."Id", 'insert', v_user, 'success', null, v_role, null, v_meta);
        return NEW;
    elsif TG_OP = 'UPDATE' then
        v_meta := jsonb_build_object('patient_id', NEW."PatientId", 'record_type', NEW."RecordType", 'key_version_old', OLD."KeyVersion", 'key_version_new', NEW."KeyVersion", 'sensitivity_old', OLD."Sensitivity", 'sensitivity_new', NEW."Sensitivity", 'content_hash_old', OLD."ContentHash", 'content_hash_new', NEW."ContentHash");
        perform log_audit_event('medical_record', NEW."Id", 'update', v_user, 'success', null, v_role, null, v_meta);
        return NEW;
    elsif TG_OP = 'DELETE' then
        v_meta := jsonb_build_object('patient_id', OLD."PatientId", 'record_type', OLD."RecordType", 'key_version', OLD."KeyVersion", 'sensitivity', OLD."Sensitivity", 'content_hash', OLD."ContentHash");
        perform log_audit_event('medical_record', OLD."Id", 'delete', v_user, 'success', null, v_role, null, v_meta);
        return OLD;
    end if;
    return null;
end; $$ language plpgsql security definer;

drop trigger if exists medicalrecords_audit_ins on "MedicalRecords";
drop trigger if exists medicalrecords_audit_upd on "MedicalRecords";
drop trigger if exists medicalrecords_audit_del on "MedicalRecords";
create trigger medicalrecords_audit_ins after insert on "MedicalRecords" for each row execute function trg_medicalrecords_audit();
create trigger medicalrecords_audit_upd after update on "MedicalRecords" for each row execute function trg_medicalrecords_audit();
create trigger medicalrecords_audit_del after delete on "MedicalRecords" for each row execute function trg_medicalrecords_audit();

-- F3. Dual-write AuditEvents -> AuditEventsPart (opt-in) ------------------------
create or replace function trg_auditevents_dual_write() returns trigger as $$
declare v_flag text := coalesce(app_context('app.audit_dual_write'), current_setting('app.audit_dual_write', true));
begin
    if v_flag = 'on' and exists (select 1 from information_schema.tables where table_name='AuditEventsPart') then
        -- Insert into partitioned table (assumes identical schema)
        perform (
            with ins as (
                insert into "AuditEventsPart" (
                    "EntityType","EntityId","Action","UserId","IPAddress","UserAgent","Metadata","Timestamp",
                    "CorrelationId","Outcome","ErrorCode","ActorRole"
                ) values (
                    NEW."EntityType", NEW."EntityId", NEW."Action", NEW."UserId", NEW."IPAddress", NEW."UserAgent", NEW."Metadata", NEW."Timestamp",
                    NEW."CorrelationId", NEW."Outcome", NEW."ErrorCode", NEW."ActorRole"
                ) returning 1
            ) select 1 from ins
        );
    end if;
    return NEW;
end; $$ language plpgsql;

drop trigger if exists auditevents_dual_write on "AuditEvents";
create trigger auditevents_dual_write after insert on "AuditEvents" for each row execute function trg_auditevents_dual_write();

comment on function trg_auditevents_dual_write() is 'Conditionally dual-writes AuditEvents into partitioned AuditEventsPart when app.audit_dual_write=on.';

-- F4. Re-encryption Batch Scheduling -------------------------------------------
do $$ begin
    if not exists (select 1 from pg_type where typname='reencrypt_batch_status') then
        create type reencrypt_batch_status as enum ('pending','in_progress','completed','failed');
    end if;
    if not exists (select 1 from pg_type where typname='reencrypt_item_status') then
        create type reencrypt_item_status as enum ('pending','processing','done','error');
    end if;
end $$;

create table if not exists "ReencryptionBatches" (
    "Id"              bigserial primary key,
    "FromKeyVersion"  int not null,
    "ToKeyVersion"    int not null,
    "TotalItems"      int,
    "ProcessedItems"  int default 0,
    "Status"          reencrypt_batch_status not null default 'pending',
    "CreatedAt"       timestamptz default now(),
    "StartedAt"       timestamptz,
    "CompletedAt"     timestamptz,
    "Error"           text,
    constraint "CK_Reenc_From_NotEqual_To" check ("FromKeyVersion" <> "ToKeyVersion")
);

create table if not exists "ReencryptionBatchItems" (
    "Id"               bigserial primary key,
    "BatchId"          bigint not null references "ReencryptionBatches" on delete cascade,
    "MedicalRecordId"  int not null references "MedicalRecords" on delete cascade,
    "OldKeyVersion"    int not null,
    "NewKeyVersion"    int not null,
    "Status"           reencrypt_item_status not null default 'pending',
    "Attempts"         int default 0,
    "LastError"        text,
    "ProcessedAt"      timestamptz,
    unique ("BatchId","MedicalRecordId")
);

create index if not exists "IX_ReencryptionBatchItems_Batch_Status" on "ReencryptionBatchItems" ("BatchId","Status");

create or replace function schedule_reencryption_batch(p_to_key_version int, p_limit int default 500) returns bigint as $$
declare v_active int := p_to_key_version; v_old int; v_batch_id bigint; begin
    if v_active is null then
        v_active := encryption_current_key();
    end if;
    if v_active is null then
        raise exception 'No active target key version available';
    end if;
    -- Create batch selecting rows from backlog view
    insert into "ReencryptionBatches" ("FromKeyVersion","ToKeyVersion","Status")
    select distinct mr."KeyVersion", v_active, 'pending'::reencrypt_batch_status
      from "MedicalRecords" mr
      where mr."DeletedAt" is null and mr."KeyVersion" <> v_active
      order by mr."KeyVersion" asc
      limit 1
    returning "Id","FromKeyVersion" into v_batch_id, v_old;

    -- Populate items
    insert into "ReencryptionBatchItems" ("BatchId","MedicalRecordId","OldKeyVersion","NewKeyVersion")
    select v_batch_id, mr."Id", mr."KeyVersion", v_active
    from "MedicalRecords" mr
    where mr."DeletedAt" is null and mr."KeyVersion" = v_old
    order by mr."Id"
    limit p_limit;

    update "ReencryptionBatches" set "TotalItems" = (select count(*) from "ReencryptionBatchItems" where "BatchId"=v_batch_id) where "Id"=v_batch_id;
    return v_batch_id;
end; $$ language plpgsql security definer;

comment on function schedule_reencryption_batch(int,int) is 'Creates a batch of MedicalRecords requiring re-encryption to target key (default current active).';

create or replace function mark_reencryption_item_done(p_item_id bigint, p_success boolean, p_error text default null) returns boolean as $$
declare v_batch_id bigint; begin
    update "ReencryptionBatchItems" set
        "Status" = case when p_success then 'done' else 'error' end::reencrypt_item_status,
        "Attempts" = "Attempts" + 1,
        "LastError" = case when p_success then null else p_error end,
        "ProcessedAt" = now()
      where "Id" = p_item_id
      returning "BatchId" into v_batch_id;
    if not found then return false; end if;
    -- Update batch progress
    update "ReencryptionBatches" b set
        "ProcessedItems" = (select count(*) from "ReencryptionBatchItems" where "BatchId"=b."Id" and "Status" in ('done','error')),
        "Status" = case when (select count(*) from "ReencryptionBatchItems" where "BatchId"=b."Id" and "Status"='pending')=0 then 'completed' else b."Status" end,
        "CompletedAt" = case when (select count(*) from "ReencryptionBatchItems" where "BatchId"=b."Id" and "Status"='pending')=0 then coalesce(b."CompletedAt", now()) else b."CompletedAt" end
      where b."Id" = v_batch_id;
    return true;
end; $$ language plpgsql security definer;

comment on function mark_reencryption_item_done(bigint,boolean,text) is 'Marks a batch item processed; updates parent batch progress & completion time.';

-- Patch Set F complete.

-- =====================================================
-- PATCH SET G: DEPRECATION DROPS, FINANCIAL ADJUSTMENTS, PATIENT MERGE/DEDUP
-- =====================================================
-- Goals:
--  G1. Drop previously deprecated JSON / array columns post-migration.
--  G2. Introduce invoice adjustment capability with summarized net totals.
--  G3. Add patient duplicate candidate tracking & merge function to consolidate records safely.

-- G1. Drop Deprecated Columns --------------------------------------------------
do $$ begin
    if exists (select 1 from information_schema.columns where table_name='Patients' and column_name='EmergencyContact') then
        alter table "Patients" drop column "EmergencyContact";
    end if;
    if exists (select 1 from information_schema.columns where table_name='Patients' and column_name='Address') then
        alter table "Patients" drop column "Address";
    end if;
    if exists (select 1 from information_schema.columns where table_name='Patients' and column_name='InsuranceInfo') then
        alter table "Patients" drop column "InsuranceInfo";
    end if;
    if exists (select 1 from information_schema.columns where table_name='Providers' and column_name='ServicesOffered') then
        alter table "Providers" drop column "ServicesOffered";
    end if;
    if exists (select 1 from information_schema.columns where table_name='Providers' and column_name='AvailabilitySchedule') then
        alter table "Providers" drop column "AvailabilitySchedule";
    end if;
end $$;

-- G2. Financial Adjustments ----------------------------------------------------
-- Enum for adjustment types
do $$ begin
    if not exists (select 1 from pg_type where typname='invoice_adjustment_type') then
        create type invoice_adjustment_type as enum ('writeoff','discount','manual_correction','refund_allocation','claim_adjustment');
    end if;
end $$;

create table if not exists "InvoiceAdjustments" (
    "Id"             bigserial primary key,
    "InvoiceId"      integer not null references "Invoices" on delete cascade,
    "Type"           invoice_adjustment_type not null,
    "Amount"         numeric(12,2) not null, -- Sign convention: negative reduces amount due; positive increases
    "Reason"         text,
    "Metadata"       jsonb,
    "CreatedBy"      text,
    "CreatedAt"      timestamptz default now(),
    "UpdatedAt"      timestamptz default now()
);

create index if not exists "IX_InvoiceAdjustments_InvoiceId" on "InvoiceAdjustments" ("InvoiceId");

-- Add aggregation columns to Invoices (does not disturb existing math constraint)
alter table "Invoices" add column if not exists "AdjustmentsTotal" numeric(12,2) default 0;
alter table "Invoices" add column if not exists "NetTotal" numeric(12,2) generated always as (coalesce("Total",0) + coalesce("AdjustmentsTotal",0)) stored;

-- UpdatedAt trigger for InvoiceAdjustments
do $$ begin
    if exists (select 1 from pg_proc where proname='set_updated_at') then
        execute 'drop trigger if exists trg_invoiceadjustments_updated on "InvoiceAdjustments"';
        execute 'create trigger trg_invoiceadjustments_updated before update on "InvoiceAdjustments" for each row execute function set_updated_at()';
    end if;
end $$;

-- Reconciliation to refresh AdjustmentsTotal and Net effect on PaidAt semantics unchanged
create or replace function reconcile_invoice_adjustments(p_invoice_id int) returns void as $$
begin
    update "Invoices" i
       set "AdjustmentsTotal" = coalesce((select sum("Amount") from "InvoiceAdjustments" ia where ia."InvoiceId"=i."Id"),0),
           "UpdatedAt" = now()
     where i."Id" = p_invoice_id;
end; $$ language plpgsql;

comment on function reconcile_invoice_adjustments(int) is 'Recomputes Invoices.AdjustmentsTotal from related InvoiceAdjustments.';

create or replace function trg_invoiceadjustments_invoice_update() returns trigger as $$
begin
    perform reconcile_invoice_adjustments(NEW."InvoiceId");
    return NEW;
end; $$ language plpgsql;

drop trigger if exists invoiceadjustments_invoice_update on "InvoiceAdjustments";
create trigger invoiceadjustments_invoice_update after insert or update of "Amount" on "InvoiceAdjustments" for each row execute function trg_invoiceadjustments_invoice_update();

-- Convenience view summarizing invoice financials
create or replace view "InvoiceFinancialSummary" as
    select i."Id", i."PatientId", i."AppointmentId", i."Status", i."SubTotal", i."Tax", i."Total", i."AdjustmentsTotal", i."NetTotal", i."PaidAmount",
           (i."NetTotal" - coalesce(i."PaidAmount",0)) as "Outstanding",
           i."Currency", i."DueDate", i."IssuedAt", i."PaidAt", i."CreatedAt", i."UpdatedAt"
    from "Invoices" i
    where i."DeletedAt" is null;

comment on view "InvoiceFinancialSummary" is 'Provides net financial position for invoices including adjustments.';

-- G3. Patient Duplicate Detection & Merge --------------------------------------
do $$ begin
    if not exists (select 1 from pg_type where typname='duplicate_candidate_status') then
        create type duplicate_candidate_status as enum ('new','reviewed','merged','dismissed');
    end if;
end $$;

create table if not exists "PatientDuplicateCandidates" (
    "Id"            bigserial primary key,
    "PatientId1"    int not null references "Patients" on delete cascade,
    "PatientId2"    int not null references "Patients" on delete cascade,
    "SimilarityScore" numeric(5,4) not null check ("SimilarityScore" between 0 and 1),
    "Status"       duplicate_candidate_status not null default 'new',
    "Reason"       text,
    "CreatedAt"    timestamptz default now(),
    "UpdatedAt"    timestamptz default now(),
    constraint "CK_PatientDuplicateCandidates_Order" check ("PatientId1" < "PatientId2"),
    unique ("PatientId1","PatientId2")
);

create index if not exists "IX_PatientDuplicateCandidates_Status" on "PatientDuplicateCandidates" ("Status");

do $$ begin
    if exists (select 1 from pg_proc where proname='set_updated_at') then
        execute 'drop trigger if exists trg_patientduplicatecandidates_updated on "PatientDuplicateCandidates"';
        execute 'create trigger trg_patientduplicatecandidates_updated before update on "PatientDuplicateCandidates" for each row execute function set_updated_at()';
    end if;
end $$;

-- Helper to insert/flag candidate
create or replace function flag_patient_duplicate(p_patient_a int, p_patient_b int, p_score numeric, p_reason text) returns bigint as $$
declare v_id bigint; begin
    if p_patient_a = p_patient_b then
        raise exception 'Cannot flag duplicate: same patient id';
    end if;
    insert into "PatientDuplicateCandidates" ("PatientId1","PatientId2","SimilarityScore","Reason")
    values (least(p_patient_a,p_patient_b), greatest(p_patient_a,p_patient_b), p_score, p_reason)
    on conflict ("PatientId1","PatientId2") do update set "SimilarityScore"=excluded."SimilarityScore", "Reason"=excluded."Reason", "UpdatedAt"=now()
    returning "Id" into v_id;
    return v_id;
end; $$ language plpgsql security definer;

comment on function flag_patient_duplicate(int,int,numeric,text) is 'Upserts a patient duplicate candidate pair with similarity score & reason.';

-- Merge function (reassigns references then soft deletes loser)
create or replace function merge_patients(p_winner int, p_loser int, p_user text) returns boolean as $$
declare v_exists_w boolean; v_exists_l boolean; begin
    if p_winner = p_loser then raise exception 'Winner and loser cannot be same'; end if;
    select true into v_exists_w from "Patients" where "Id"=p_winner and "DeletedAt" is null;
    select true into v_exists_l from "Patients" where "Id"=p_loser and "DeletedAt" is null;
    if not v_exists_w or not v_exists_l then raise exception 'Winner or loser patient not found (or already deleted)'; end if;
    -- Lock both rows to avoid concurrent merges
    perform 1 from "Patients" where "Id" in (p_winner,p_loser) for update;

    -- Reassign direct foreign keys
    update "Appointments" set "PatientId"=p_winner where "PatientId"=p_loser;
    update "MedicalRecords" set "PatientId"=p_winner where "PatientId"=p_loser;
    update "PaymentMethods" set "PatientId"=p_winner where "PatientId"=p_loser;
    update "Invoices" set "PatientId"=p_winner where "PatientId"=p_loser;

    -- PatientAddresses (composite key) - insert missing then delete loser
    insert into "PatientAddresses" ("PatientId","AddressId","Type","IsPrimary","ValidFrom","ValidTo","CreatedAt","UpdatedAt","DeletedAt","DeletedBy")
    select p_winner, "AddressId","Type","IsPrimary","ValidFrom","ValidTo","CreatedAt","UpdatedAt","DeletedAt","DeletedBy"
    from "PatientAddresses" where "PatientId"=p_loser
    on conflict do nothing;
    delete from "PatientAddresses" where "PatientId"=p_loser;

    -- Insurance policies
    insert into "PatientInsurancePolicies" ("PatientId","Carrier","PolicyNumber","GroupNumber","PlanType","EffectiveDate","ExpiryDate","Metadata","CreatedAt","UpdatedAt","DeletedAt","DeletedBy")
    select p_winner,"Carrier","PolicyNumber","GroupNumber","PlanType","EffectiveDate","ExpiryDate","Metadata","CreatedAt","UpdatedAt","DeletedAt","DeletedBy"
    from "PatientInsurancePolicies" where "PatientId"=p_loser
    on conflict ("PatientId","PolicyNumber") do nothing;
    delete from "PatientInsurancePolicies" where "PatientId"=p_loser;

    -- Contacts
    insert into "PatientContacts" ("PatientId","Name","Relationship","Type","Phone","Email","AddressId","Preferred","CreatedAt","UpdatedAt","DeletedAt","DeletedBy")
    select p_winner,"Name","Relationship","Type","Phone","Email","AddressId","Preferred","CreatedAt","UpdatedAt","DeletedAt","DeletedBy"
    from "PatientContacts" where "PatientId"=p_loser;
    delete from "PatientContacts" where "PatientId"=p_loser;

    -- Mark duplicate candidate rows merged
    update "PatientDuplicateCandidates" set "Status"='merged', "UpdatedAt"=now()
      where ("PatientId1"=least(p_winner,p_loser) and "PatientId2"=greatest(p_winner,p_loser));

    -- Soft delete loser
    perform soft_delete_record('Patients', p_loser, p_user);

    -- Audit
    perform log_audit_event('patient', p_winner, 'merge_winner', p_user, 'success', null, app_context('app.role'), null,
        jsonb_build_object('merged_from', p_loser));
    perform log_audit_event('patient', p_loser, 'merge_loser', p_user, 'success', null, app_context('app.role'), null,
        jsonb_build_object('merged_into', p_winner));
    return true;
end; $$ language plpgsql security definer;

comment on function merge_patients(int,int,text) is 'Merges loser patient into winner: reassigns FKs, consolidates related data, soft deletes loser, audits actions.';

-- Convenience view listing unresolved duplicates
create or replace view "PatientDuplicateOpen" as
    select * from "PatientDuplicateCandidates" where "Status" in ('new','reviewed');

comment on view "PatientDuplicateOpen" is 'Lists patient duplicate candidates pending resolution.';

-- Patch Set G complete.

-- =====================================================
-- PATCH SET H: PARTITION MIGRATION, OUTBOX DEAD LETTERS, CLAIM DENIAL TAXONOMY
-- =====================================================
-- Goals:
--  H1. Partition migration scaffolding & dual-write for high-volume tables (AuditEvents already prepared; now OutboxEvents & InboundEvents).
--  H2. Dead letter handling for failed Outbox events.
--  H3. Structured claims denial taxonomy with reference codes & helper for recording denials.

-- H1. Partitioned Parent Tables & Dual-Write -----------------------------------
-- Create partitioned variants if not present. We reuse ensure_month_partition(regclass,text,date).

do $$ begin
    if not exists (select 1 from information_schema.tables where table_name='OutboxEventsPart') then
        execute 'create table "OutboxEventsPart" (like "OutboxEvents" including all) partition by range ("OccurredAt")';
    end if;
    if not exists (select 1 from information_schema.tables where table_name='InboundEventsPart') then
        execute 'create table "InboundEventsPart" (like "InboundEvents" including all) partition by range ("ReceivedAt")';
    end if;
end $$;

-- Seed current & next month partitions for each partitioned parent
do $$ begin
    if exists (select 1 from information_schema.tables where table_name='OutboxEventsPart') then
        perform ensure_month_partition('OutboxEventsPart'::regclass,'OccurredAt', current_date);
        perform ensure_month_partition('OutboxEventsPart'::regclass,'OccurredAt', current_date + interval '1 month');
    end if;
    if exists (select 1 from information_schema.tables where table_name='InboundEventsPart') then
        perform ensure_month_partition('InboundEventsPart'::regclass,'ReceivedAt', current_date);
        perform ensure_month_partition('InboundEventsPart'::regclass,'ReceivedAt', current_date + interval '1 month');
    end if;
end $$;

-- Dual-write triggers gated by GUC flags (app.outbox_dual_write, app.inbound_dual_write)
create or replace function trg_outboxevents_dual_write() returns trigger as $$
declare v_flag text := coalesce(app_context('app.outbox_dual_write'), current_setting('app.outbox_dual_write', true));
begin
    if v_flag = 'on' and exists (select 1 from information_schema.tables where table_name='OutboxEventsPart') then
        perform (
            with ins as (
                insert into "OutboxEventsPart" (
                    "EventType","AggregateType","AggregateId","Payload","OccurredAt","Status","ProcessedAt","RetryCount","ErrorMessage","CorrelationId","SchemaVersion","CreatedAt","UpdatedAt"
                ) values (
                    NEW."EventType",NEW."AggregateType",NEW."AggregateId",NEW."Payload",NEW."OccurredAt",NEW."Status",NEW."ProcessedAt",NEW."RetryCount",NEW."ErrorMessage",NEW."CorrelationId",NEW."SchemaVersion",NEW."CreatedAt",NEW."UpdatedAt"
                ) returning 1
            ) select 1 from ins
        );
    end if;
    return NEW;
end; $$ language plpgsql;

drop trigger if exists outboxevents_dual_write on "OutboxEvents";
create trigger outboxevents_dual_write after insert on "OutboxEvents" for each row execute function trg_outboxevents_dual_write();

create or replace function trg_inboundevents_dual_write() returns trigger as $$
declare v_flag text := coalesce(app_context('app.inbound_dual_write'), current_setting('app.inbound_dual_write', true));
begin
    if v_flag = 'on' and exists (select 1 from information_schema.tables where table_name='InboundEventsPart') then
        perform (
            with ins as (
                insert into "InboundEventsPart" (
                    "SourceSystem","EventType","ExternalId","DedupKey","Status","Payload","ReceivedAt","ProcessedAt","Error","CorrelationId","Metadata","CreatedAt","UpdatedAt"
                ) values (
                    NEW."SourceSystem",NEW."EventType",NEW."ExternalId",NEW."DedupKey",NEW."Status",NEW."Payload",NEW."ReceivedAt",NEW."ProcessedAt",NEW."Error",NEW."CorrelationId",NEW."Metadata",NEW."CreatedAt",NEW."UpdatedAt"
                ) returning 1
            ) select 1 from ins
        );
    end if;
    return NEW;
end; $$ language plpgsql;

drop trigger if exists inboundevents_dual_write on "InboundEvents";
create trigger inboundevents_dual_write after insert on "InboundEvents" for each row execute function trg_inboundevents_dual_write();

comment on function trg_outboxevents_dual_write() is 'Conditional dual-write of OutboxEvents to partitioned OutboxEventsPart when app.outbox_dual_write=on.';
comment on function trg_inboundevents_dual_write() is 'Conditional dual-write of InboundEvents to partitioned InboundEventsPart when app.inbound_dual_write=on.';

-- Optional cutover helper: copy remaining rows and then swap (manual final rename suggested outside this static script)
create or replace function partition_backfill_progress(p_source regclass, p_target regclass) returns table(total bigint, copied bigint) as $$
declare v_sql text; v_total bigint; v_copied bigint; begin
    execute format('select count(*) from %s', p_source) into v_total;
    execute format('select count(*) from %s', p_target) into v_copied;
    return query select v_total, v_copied;
end; $$ language plpgsql stable;

comment on function partition_backfill_progress(regclass,regclass) is 'Reports total vs copied row counts for migration monitoring.';

-- H2. Outbox Dead Letters ------------------------------------------------------
create table if not exists "OutboxDeadLetters" (
    "Id"            bigserial primary key,
    "OriginalEventId" bigint not null,
    "EventType"     text not null,
    "AggregateType" text not null,
    "AggregateId"   bigint not null,
    "Payload"       jsonb not null,
    "FailedAt"      timestamptz not null default now(),
    "ErrorMessage"  text,
    "RetryCount"    int,
    "LastStatus"    outbox_status,
    "CorrelationId" uuid,
    "Metadata"      jsonb,
    "CreatedAt"     timestamptz default now()
);

create index if not exists "IX_OutboxDeadLetters_FailedAt" on "OutboxDeadLetters" ("FailedAt" desc);

comment on table "OutboxDeadLetters" is 'Archive of irrecoverably failed Outbox events retained for analysis & manual replay.';

create or replace function move_outbox_to_dead_letter(p_event_id bigint, p_error text) returns boolean as $$
declare r record; begin
    select * into r from "OutboxEvents" where "Id"=p_event_id;
    if not found then return false; end if;
    insert into "OutboxDeadLetters" ("OriginalEventId","EventType","AggregateType","AggregateId","Payload","ErrorMessage","RetryCount","LastStatus","CorrelationId")
    values (r."Id", r."EventType", r."AggregateType", r."AggregateId", r."Payload", coalesce(p_error, r."ErrorMessage"), r."RetryCount", r."Status", r."CorrelationId");
    delete from "OutboxEvents" where "Id"=p_event_id;
    return true;
end; $$ language plpgsql security definer;

comment on function move_outbox_to_dead_letter(bigint,text) is 'Moves an OutboxEvents row to OutboxDeadLetters and deletes the original.';

-- Trigger to auto-move failed events when exceeding retry threshold (e.g., 10) if configured
create or replace function trg_outbox_fail_to_dead_letter() returns trigger as $$
declare v_threshold int := coalesce(current_setting('app.outbox_deadletter_threshold', true)::int, 10);
begin
    if NEW."Status"='failed' and NEW."RetryCount" >= v_threshold then
        perform move_outbox_to_dead_letter(NEW."Id", NEW."ErrorMessage");
        return null; -- row deleted; suppress returning NEW
    end if;
    return NEW;
end; $$ language plpgsql;

drop trigger if exists outbox_fail_to_dead_letter on "OutboxEvents";
create trigger outbox_fail_to_dead_letter after update of "Status","RetryCount" on "OutboxEvents" for each row when (NEW."Status"='failed') execute function trg_outbox_fail_to_dead_letter();

comment on function trg_outbox_fail_to_dead_letter() is 'Automatically moves failed OutboxEvents to dead letters when retry threshold reached.';

-- H3. Claims Denial Taxonomy ---------------------------------------------------
create table if not exists "ClaimDenialReasons" (
    "Code"          text primary key,          -- e.g., PR-1, CO-16 (payer remittance codes) or internal codes
    "Category"      text not null,             -- patient_responsibility, contractual, coding, eligibility, timing, duplicate, other
    "Severity"      smallint not null check ("Severity" between 1 and 5),
    "Description"   text not null,
    "Guidance"      text,                     -- remediation hints
    "ExternalMapping" jsonb,                  -- payer-specific or X12 mapping
    "CreatedAt"     timestamptz default now(),
    "UpdatedAt"     timestamptz default now()
);

create index if not exists "IX_ClaimDenialReasons_Category" on "ClaimDenialReasons" ("Category");

do $$ begin
    if exists (select 1 from pg_proc where proname='set_updated_at') then
        execute 'drop trigger if exists trg_claimdenialreasons_updated on "ClaimDenialReasons"';
        execute 'create trigger trg_claimdenialreasons_updated before update on "ClaimDenialReasons" for each row execute function set_updated_at()';
    end if;
end $$;

-- Seed a few common denial codes (idempotent examples)
insert into "ClaimDenialReasons" ("Code","Category","Severity","Description","Guidance") values
    ('CO-16','eligibility',3,'Claim/service lacks information or has submission/billing error','Verify patient eligibility; correct missing/invalid data and resubmit'),
    ('PR-1','patient_responsibility',2,'Deductible amount','Bill patient deductible portion'),
    ('CO-97','contractual',3,'Procedure not paid separately','Review bundling rules'),
    ('CO-109','coding',4,'Claim not covered by this payer/contractor','Confirm payer selection and contract')
on conflict ("Code") do nothing;

-- Link to InsuranceClaims (add DenialReasonCode if not exists)
alter table "InsuranceClaims" add column if not exists "DenialReasonCode" text references "ClaimDenialReasons"("Code") on delete set null;
comment on column "InsuranceClaims"."DenialReason" is 'Free-text denial explanation; prefer DenialReasonCode referencing ClaimDenialReasons.';

-- Helper to record a denial with taxonomy
create or replace function record_claim_denial(p_claim_id int, p_reason_code text, p_free_text text default null, p_user text default null) returns boolean as $$
declare v_exists boolean; begin
    select true into v_exists from "InsuranceClaims" where "Id"=p_claim_id;
    if not v_exists then return false; end if;
    update "InsuranceClaims"
       set "Status"='denied', "DenialReasonCode"=p_reason_code, "DenialReason"=coalesce(p_free_text, "DenialReason"), "UpdatedAt"=now()
     where "Id"=p_claim_id;
    -- History trigger already captures status change; add audit
    perform log_audit_event('insurance_claim', p_claim_id, 'denial', coalesce(p_user, app_context('app.current_user_id')), 'success', null, app_context('app.role'), null,
        jsonb_build_object('reason_code', p_reason_code, 'free_text', p_free_text));
    return true;
end; $$ language plpgsql security definer;

comment on function record_claim_denial(int,text,text,text) is 'Updates claim to denied with standardized reason code + optional free text; audits action.';

-- View summarizing denial metrics
create or replace view "ClaimDenialsSummary" as
    select r."Code", r."Category", r."Severity", r."Description",
           count(c.*) filter (where c."Status"='denied') as denied_count,
           count(c.*) as total_claims,
           round( (count(c.*) filter (where c."Status"='denied')::numeric / nullif(count(c.*),0)) * 100, 2) as denial_rate_percent
    from "ClaimDenialReasons" r
    left join "InsuranceClaims" c on c."DenialReasonCode" = r."Code"
    group by r."Code", r."Category", r."Severity", r."Description";

comment on view "ClaimDenialsSummary" is 'Aggregated denial counts & rates per denial reason code.';

-- Patch Set H complete.

-- =====================================================
-- PATCH SET I: PARTITION CUTOVER PROCEDURES & SLA / WORK ITEMS
-- =====================================================
-- Goals:
--  I1. Provide operational functions to migrate from row-store base tables to partitioned parents (AuditEventsPart, OutboxEventsPart, InboundEventsPart).
--  I2. Supply batched backfill + verification + final cutover helpers (non-destructive until final step).
--  I3. Introduce SLA & Work Item tracking with status history and breach monitoring.

-- I1 / I2. Partition Migration Helpers -----------------------------------------
-- NOTE: These helpers assume dual-write triggers already active (Patch Sets E/H) via GUC flags.
-- Strategy:
--   1. enable dual-write (set GUC in app layer) to partitioned parent.
--   2. iterative calls to backfill function until source rows (pre-cutoff) fully copied.
--   3. run verify; schedule short downtime window.
--   4. call finalize_partition_cutover to move remaining tail rows & rename tables.
--   5. (Optional) create view <OldName>_legacy pointing to old table for audit.

create or replace function backfill_partitions(
    p_source regclass,
    p_parent regclass,
    p_ts_column text,
    p_batch_limit int default 100000,
    p_max_age interval default null -- if provided, only rows older than now()-p_max_age are copied this pass
) returns integer as $$
declare v_sql text; v_copied int; v_filter text := '1=1'; v_src text := (select relname from pg_class where oid=p_source);
        v_parent text := (select relname from pg_class where oid=p_parent); r record; begin
    if p_max_age is not null then
        v_filter := format('%I < now() - %L::interval', p_ts_column, p_max_age::text);
    end if;
    -- Copy batch (do not delete yet; tail sync done at finalize)
    for r in execute format(
        'select *, %I as ts_col from %s where %s order by %I asc limit %s',
        p_ts_column, p_source, v_filter, p_ts_column, p_batch_limit) loop
        perform ensure_month_partition(p_parent, p_ts_column, r.ts_col::date);
        execute format('insert into %s select ($1).* on conflict do nothing', p_parent) using r; -- relies on identical structure
        v_copied := coalesce(v_copied,0)+1;
    end loop;
    return coalesce(v_copied,0);
end; $$ language plpgsql;

comment on function backfill_partitions(regclass,regclass,text,int,interval) is 'Copies up to batch_limit rows from source to partition parent (no delete).';

create or replace function verify_partition_sync(p_source regclass, p_parent regclass) returns table(source_count bigint, parent_count bigint, delta bigint) as $$
declare v_source bigint; v_parent bigint; begin
    execute format('select count(*) from %s', p_source) into v_source;
    execute format('select count(*) from %s', p_parent) into v_parent;
    return query select v_source, v_parent, (v_source - v_parent);
end; $$ language plpgsql stable;

comment on function verify_partition_sync(regclass,regclass) is 'Returns counts & delta between source and partition parent prior to cutover.';

create or replace function finalize_partition_cutover(
    p_source regclass,
    p_parent regclass,
    p_ts_column text,
    p_lock_timeout_ms int default 5000,
    p_drop_original boolean default false
) returns boolean as $$
declare v_src text := (select relname from pg_class where oid=p_source);
        v_parent text := (select relname from pg_class where oid=p_parent);
        v_backup text := v_src || '_legacy';
        v_delta bigint; r record; begin
    perform set_config('lock_timeout', p_lock_timeout_ms::text, true);
    -- Lock source to quiesce writes (assumes application paused or writes re-routed)
    execute format('lock table %s in access exclusive mode', p_source);
    -- Copy any remaining rows not yet present (left join anti join on primary key if possible). We assume simple PK Id.
    for r in execute format('select s.* from %s s left join %s p on s."Id"=p."Id" where p."Id" is null', p_source, p_parent) loop
        perform ensure_month_partition(p_parent, p_ts_column, (r)."'||p_ts_column||'"::date);
        execute format('insert into %s select ($1).*', p_parent) using r;
    end loop;
    -- Verify counts
    select delta into v_delta from verify_partition_sync(p_source, p_parent);
    if v_delta <> 0 then
        raise exception 'Cutover aborted: count delta % differs from 0', v_delta;
    end if;
    -- Rename: source -> *_legacy, parent -> original name
    execute format('alter table %s rename to %s', v_src, v_backup);
    execute format('alter table %s rename to %s', v_parent, v_src);
    -- Create view referencing legacy if not dropped
    if not p_drop_original then
        execute format('create or replace view %s_legacy_view as select * from %s', v_src, v_backup);
    else
        -- Optionally drop legacy table after rename (dangerous) -> user may manually DROP later
        null;
    end if;
    return true;
end; $$ language plpgsql;

comment on function finalize_partition_cutover(regclass,regclass,text,int,boolean) is 'Performs final sync, validates counts, renames parent to source, and archives legacy table.';

-- I3. SLA / Work Items ---------------------------------------------------------
do $$ begin
    if not exists (select 1 from pg_type where typname='work_item_status') then
        create type work_item_status as enum ('open','in_progress','blocked','completed','cancelled','expired');
    end if;
    if not exists (select 1 from pg_type where typname='work_item_priority') then
        create type work_item_priority as enum ('low','normal','high','urgent');
    end if;
end $$;

create table if not exists "SlaDefinitions" (
    "Id"            serial primary key,
    "Name"          text not null unique,
    "EntityType"    text not null,              -- appointment, claim, workflow_state, etc.
    "Condition"     jsonb,                     -- rule metadata (e.g. required state) evaluated in app layer or stored proc
    "TargetSeconds" integer not null check ("TargetSeconds" > 0),
    "DefaultPriority" work_item_priority default 'normal',
    "IsActive"      boolean default true,
    "CreatedAt"     timestamptz default now(),
    "UpdatedAt"     timestamptz default now()
);

create table if not exists "WorkItems" (
    "Id"             bigserial primary key,
    "EntityType"     text not null,
    "EntityId"       bigint not null,
    "Type"           text not null,              -- classification (follow_up, prior_auth, review, investigation)
    "Status"         work_item_status not null default 'open',
    "Priority"       work_item_priority not null default 'normal',
    "AssignedTo"     text,
    "SlaDefinitionId" integer references "SlaDefinitions" on delete set null,
    "SLASecondsTarget" integer,                 -- snapshot of target for this item
    "DueAt"          timestamptz,               -- optional explicit due
    "BreachAt"       timestamptz,               -- computed from CreatedAt + SLASecondsTarget
    "StartedAt"      timestamptz,
    "CompletedAt"    timestamptz,
    "CancelledAt"    timestamptz,
    "BlockedAt"      timestamptz,
    "Metadata"       jsonb,
    "CreatedAt"      timestamptz default now(),
    "UpdatedAt"      timestamptz default now(),
    unique ("EntityType","EntityId","Type")
);

create table if not exists "WorkItemStatusHistory" (
    "Id"          bigserial primary key,
    "WorkItemId"  bigint not null references "WorkItems" on delete cascade,
    "FromStatus"  work_item_status,
    "ToStatus"    work_item_status not null,
    "ChangedAt"   timestamptz default now(),
    "ChangedBy"   text,
    "Reason"      text
);

create index if not exists "IX_WorkItems_Status_Priority" on "WorkItems" ("Status","Priority");
create index if not exists "IX_WorkItems_BreachAt" on "WorkItems" ("BreachAt") where "Status" in ('open','in_progress','blocked');
create index if not exists "IX_WorkItemStatusHistory_Item" on "WorkItemStatusHistory" ("WorkItemId","ChangedAt" desc);

do $$ begin
    if exists (select 1 from pg_proc where proname='set_updated_at') then
        execute 'drop trigger if exists trg_sladefinitions_updated on "SlaDefinitions"';
        execute 'create trigger trg_sladefinitions_updated before update on "SlaDefinitions" for each row execute function set_updated_at()';
        execute 'drop trigger if exists trg_workitems_updated on "WorkItems"';
        execute 'create trigger trg_workitems_updated before update on "WorkItems" for each row execute function set_updated_at()';
    end if;
end $$;

create or replace function trg_workitems_status_history() returns trigger as $$
begin
    if TG_OP = 'UPDATE' and NEW."Status" is distinct from OLD."Status" then
        insert into "WorkItemStatusHistory" ("WorkItemId","FromStatus","ToStatus","ChangedBy")
        values (OLD."Id", OLD."Status", NEW."Status", app_context('app.current_user_id'));
    end if;
    return NEW;
end; $$ language plpgsql;

drop trigger if exists workitems_status_history on "WorkItems";
create trigger workitems_status_history after update of "Status" on "WorkItems" for each row execute function trg_workitems_status_history();

create or replace function create_work_item(
    p_entity_type text,
    p_entity_id bigint,
    p_type text,
    p_priority work_item_priority default 'normal',
    p_sla_seconds int default null,
    p_assigned_to text default null,
    p_metadata jsonb default '{}'::jsonb
) returns bigint as $$
declare v_id bigint; v_now timestamptz := now(); v_breach timestamptz; begin
    if p_sla_seconds is not null then
        v_breach := v_now + make_interval(secs => p_sla_seconds);
    end if;
    insert into "WorkItems" ("EntityType","EntityId","Type","Priority","SLASecondsTarget","BreachAt","AssignedTo","Metadata")
        values (p_entity_type,p_entity_id,p_type,p_priority,p_sla_seconds,v_breach,p_assigned_to,p_metadata)
        on conflict ("EntityType","EntityId","Type") do update set "Priority"=excluded."Priority" returning "Id" into v_id;
    return v_id;
end; $$ language plpgsql security definer;

comment on function create_work_item(text,bigint,text,work_item_priority,int,text,jsonb) is 'Creates or upserts a work item with optional SLA target; returns WorkItem Id.';

create or replace function mark_work_item_status(p_work_item_id bigint, p_new_status work_item_status, p_user text default null) returns boolean as $$
declare v_prev work_item_status; begin
    update "WorkItems" set "Status"=p_new_status,
        "StartedAt" = case when p_new_status='in_progress' and "StartedAt" is null then now() else "StartedAt" end,
        "CompletedAt" = case when p_new_status='completed' then now() else "CompletedAt" end,
        "CancelledAt" = case when p_new_status='cancelled' then now() else "CancelledAt" end,
        "BlockedAt" = case when p_new_status='blocked' then now() else "BlockedAt" end,
        "UpdatedAt"=now()
        where "Id"=p_work_item_id returning "Status" into v_prev;
    return found;
end; $$ language plpgsql security definer;

comment on function mark_work_item_status(bigint,work_item_status,text) is 'Updates work item status, timestamps transitions accordingly.';

create or replace function refresh_workitem_breaches() returns integer as $$
declare v_count int; begin
    update "WorkItems" set "Status"='expired', "UpdatedAt"=now()
      where "Status" in ('open','in_progress','blocked') and "BreachAt" is not null and now() > "BreachAt";
    GET DIAGNOSTICS v_count = ROW_COUNT;
    return v_count;
end; $$ language plpgsql security definer;

comment on function refresh_workitem_breaches() is 'Expires work items whose BreachAt has passed; returns count updated.';

-- SLA convenience view
create or replace view "WorkItems_Open" as
    select * from "WorkItems" where "Status" in ('open','in_progress','blocked') order by coalesce("BreachAt", now()) asc, "Priority" desc;

comment on view "WorkItems_Open" is 'Active work items prioritized by impending breach and priority.';

-- Patch Set I complete.

-- =====================================================
-- PATCH SET J: ANALYTICS MATERIALIZED VIEWS & REFRESH SCHEDULING
-- =====================================================
-- Goals:
--  J1. Create core operational analytics materialized views (daily appointments, revenue, provider utilization, claim denial rates, SLA performance).
--  J2. Provide a unified refresh function supporting concurrent refresh when possible.
--  J3. Optional automated scheduling using pg_cron (idempotent scaffolding; requires superuser to install extension).

-- J1. Materialized Views -------------------------------------------------------
-- Note: Use WITH NO DATA initially for fast creation; call refresh_all_materialized_views() to populate.

-- Daily Appointments Metrics
create materialized view if not exists "mv_daily_appointments" as
select date_trunc('day', a."ScheduledAt")::date as "Day",
       count(*) filter (where a."Status" = 'scheduled') as scheduled_count,
       count(*) filter (where a."Status" = 'completed') as completed_count,
       count(*) filter (where a."Status" = 'cancelled') as cancelled_count,
       avg( extract(epoch from (a."CompletedAt" - a."StartedAt")) / 60.0 ) filter (where a."CompletedAt" is not null and a."StartedAt" is not null) as avg_duration_minutes
from "Appointments" a
where a."DeletedAt" is null
group by 1
order by 1
with no data;

create unique index if not exists "UQ_mv_daily_appointments_day" on "mv_daily_appointments" ("Day");

-- Daily Revenue Metrics (Invoice financials)
create materialized view if not exists "mv_daily_revenue" as
select date_trunc('day', coalesce(i."IssuedAt", i."CreatedAt"))::date as "Day",
       sum(coalesce(i."Total",0)) as total_invoiced,
       sum(coalesce(i."AdjustmentsTotal",0)) as total_adjustments,
       sum(coalesce(i."NetTotal",0)) as total_net,
       sum(coalesce(i."PaidAmount",0)) as total_paid,
       sum( greatest(coalesce(i."NetTotal",0) - coalesce(i."PaidAmount",0),0) ) as outstanding_amount
from "Invoices" i
where i."DeletedAt" is null
group by 1
order by 1
with no data;

create unique index if not exists "UQ_mv_daily_revenue_day" on "mv_daily_revenue" ("Day");

-- Provider Utilization (slots & appointments)
create materialized view if not exists "mv_provider_utilization" as
with slots as (
    select "ProviderId", date_trunc('day', "StartTime")::date as day,
           count(*) filter (where "Status"='available') as slots_available,
           count(*) filter (where "Status"='booked') as slots_booked
    from "ProviderTimeSlots"
    where "DeletedAt" is null
    group by 1,2
), appts as (
    select "ProviderId", date_trunc('day', "ScheduledAt")::date as day,
           count(*) filter (where "Status"='completed') as completed_appts
    from "Appointments"
    where "DeletedAt" is null and "ProviderId" is not null
    group by 1,2
)
select s."ProviderId", s.day as "Day", s.slots_available, s.slots_booked,
       a.completed_appts,
       case when s.slots_available > 0 then round((s.slots_booked::numeric / s.slots_available) * 100,2) else null end as utilization_pct
from slots s
left join appts a on a."ProviderId" = s."ProviderId" and a.day = s.day
order by s."ProviderId", s.day
with no data;

create unique index if not exists "UQ_mv_provider_utilization_provider_day" on "mv_provider_utilization" ("ProviderId","Day");

-- Claims Denial Rates Daily
create materialized view if not exists "mv_claims_denial_rates" as
select date_trunc('day', coalesce(c."ResponseAt", c."CreatedAt"))::date as "Day",
       count(*) as total_claims,
       count(*) filter (where c."Status"='denied') as denied_claims,
       round( (count(*) filter (where c."Status"='denied')::numeric / nullif(count(*),0)) * 100, 2) as denial_rate_pct
from "InsuranceClaims" c
group by 1
order by 1
with no data;

create unique index if not exists "UQ_mv_claims_denial_rates_day" on "mv_claims_denial_rates" ("Day");

-- Work Item SLA Performance (daily aggregates of completions & breaches)
create materialized view if not exists "mv_workitem_sla" as
with base as (
    select date_trunc('day', w."CreatedAt")::date as day,
           count(*) as opened,
           count(*) filter (where w."CompletedAt" is not null) as completed,
           count(*) filter (where w."Status"='expired') as expired_total,
           percentile_cont(0.5) within group (order by extract(epoch from (w."CompletedAt" - w."CreatedAt"))) filter (where w."CompletedAt" is not null) as median_time_to_complete_seconds
    from "WorkItems" w
    group by 1
)
select day as "Day", opened, completed, expired_total as breaches,
       case when opened > 0 then round((expired_total::numeric / opened) * 100,2) else null end as breach_rate_pct,
       median_time_to_complete_seconds
from base
order by day
with no data;

create unique index if not exists "UQ_mv_workitem_sla_day" on "mv_workitem_sla" ("Day");

comment on materialized view "mv_daily_appointments" is 'Daily appointment counts & average duration.';
comment on materialized view "mv_daily_revenue" is 'Daily revenue, adjustments, net, payments, outstanding balance.';
comment on materialized view "mv_provider_utilization" is 'Provider daily slot availability, bookings, utilization %, and completed appointments.';
comment on materialized view "mv_claims_denial_rates" is 'Daily claim volume & denial rate.';
comment on materialized view "mv_workitem_sla" is 'Daily SLA performance: opened, completed, breaches, median completion time.';

-- J2. Unified Refresh Function --------------------------------------------------
create or replace function refresh_all_materialized_views(p_concurrent boolean default true) returns void as $$
declare r record; v_stmt text; begin
    for r in select matviewname from pg_matviews where schemaname='public' and matviewname like 'mv_%' order by matviewname loop
        begin
            if p_concurrent then
                v_stmt := format('refresh materialized view concurrently %I', r.matviewname);
            else
                v_stmt := format('refresh materialized view %I', r.matviewname);
            end if;
            execute v_stmt;
        exception when others then
            -- Fallback to non-concurrent if concurrent failed (likely missing unique index)
            if p_concurrent then
                begin
                    v_stmt := format('refresh materialized view %I', r.matviewname);
                    execute v_stmt; -- second attempt
                exception when others then
                    raise notice 'Refresh failed for %: %', r.matviewname, SQLERRM;
                end;
            else
                raise notice 'Refresh failed for %: %', r.matviewname, SQLERRM;
            end if;
        end;
    end loop;
end; $$ language plpgsql;

comment on function refresh_all_materialized_views(boolean) is 'Refreshes all analytics materialized views (names prefixed mv_). Attempts concurrent when requested.';

-- J3. Scheduling (pg_cron) ------------------------------------------------------
-- Attempt to create pg_cron extension if available; may require superuser. Safe wrapped in DO.
do $$ begin
    begin
        perform 1 from pg_extension where extname='pg_cron';
        if not found then
            execute 'create extension if not exists pg_cron';
        end if;
    exception when insufficient_privilege then
        raise notice 'Insufficient privilege to create pg_cron extension; skipping.';
    end;
end $$;

-- Optional: schedule nightly refresh at 01:10 UTC & hourly lightweight refresh
-- (Idempotent inserts using cron.job table if available; ignore if pg_cron absent)
do $$ begin
    if exists (select 1 from pg_extension where extname='pg_cron') then
        -- Nightly full concurrent refresh
        begin
            insert into cron.job (schedule, command, nodename, nodeport, database, username, active, jobname)
            values ('10 1 * * *', 'select refresh_all_materialized_views(true);', null, null, current_database(), current_user, true, 'mv_refresh_nightly');
        exception when unique_violation then null; end;
        -- Hourly incremental (non-concurrent fallback if locking issues)
        begin
            insert into cron.job (schedule, command, nodename, nodeport, database, username, active, jobname)
            values ('5 * * * *', 'select refresh_all_materialized_views(true);', null, null, current_database(), current_user, true, 'mv_refresh_hourly');
        exception when unique_violation then null; end;
    end if;
end $$;

-- Patch Set J complete.

-- =====================================================
-- PATCH SET K: POSTGIS / GEOSPATIAL ENABLEMENT
-- =====================================================
-- Goals:
--  K1. Enable PostGIS (idempotent) for spatial queries.
--  K2. Add geometry/geography columns for Facilities & ServiceAreas (canonical SRID 4326 WGS84).
--  K3. Populate geometry from existing columns (Facilities.Coordinates point, ServiceAreas.Definition GeoJSON) when possible.
--  K4. Provide helper functions for proximity, containment, and coverage queries.
--  K5. Add spatial indexes & views.
--  K6. Synchronization triggers to keep geometry in step with source JSON/point fields.

-- K1. Enable PostGIS -----------------------------------------------------------
do $$ begin
    begin
        create extension if not exists postgis;          -- geometry/geography types & functions
        create extension if not exists postgis_topology; -- optional, ignore if insufficient privilege
    exception when insufficient_privilege then
        raise notice 'Insufficient privilege to create PostGIS extensions; spatial features limited.';
    end;
end $$;

-- K2. Geometry Columns ---------------------------------------------------------
-- Facilities: add Geom (geometry(Point,4326)) & Geography (optional) for distance queries
alter table "Facilities" add column if not exists "Geom" geometry(Point,4326);
alter table "Facilities" add column if not exists "Geog" geography(Point,4326);

-- ServiceAreas: add Geom (Polygon/MultiPolygon) and Geog for coverage
alter table "ServiceAreas" add column if not exists "Geom" geometry(Geometry,4326); -- flexible to accept Polygon/Multi
alter table "ServiceAreas" add column if not exists "Geog" geography(Geometry,4326);

-- K3. Populate geometry from legacy columns ------------------------------------
-- Assumes Facilities."Coordinates" stores (longitude,latitude) OR (x,y). If unknown, attempt lat when abs(y) <= 90.
do $$ begin
    -- Facilities
    if exists (select 1 from information_schema.columns where table_name='Facilities' and column_name='Coordinates') then
        update "Facilities" f
           set "Geom" = case when f."Coordinates" is not null then
                     st_setsrid(st_makepoint( (f."Coordinates"[0])::float8, (f."Coordinates"[1])::float8 ),4326) end,
               "Geog" = case when f."Coordinates" is not null then
                     st_geographyfromtext(format('SRID=4326;POINT(%s %s)', (f."Coordinates"[0])::float8, (f."Coordinates"[1])::float8)) end
         where f."Geom" is null;
    end if;
    -- ServiceAreas from GeoJSON in Definition
    update "ServiceAreas" sa
       set "Geom" = case when sa."Definition" ? 'type' then ST_SetSRID(ST_GeomFromGeoJSON(sa."Definition"::text),4326) end,
           "Geog" = case when sa."Definition" ? 'type' then ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(sa."Definition"::text),4326),4326)::geography end
     where sa."Geom" is null and sa."Definition" is not null;
exception when undefined_function then
    raise notice 'PostGIS not available; skipping geometry population.';
end $$;

-- K4. Spatial Helper Functions -------------------------------------------------
-- Nearest facilities to a lat/long (returns Id, distance_meters)
create or replace function nearest_facilities(p_lat double precision, p_lon double precision, p_limit int default 10)
returns table("FacilityId" int, "DistanceMeters" double precision) as $$
begin
    return query
    select f."Id", ST_Distance(f."Geog", ST_MakePoint(p_lon, p_lat)::geography) as dist
    from "Facilities" f
    where f."Geog" is not null and f."IsActive" = true
    order by dist asc
    limit p_limit;
end; $$ language plpgsql stable;

comment on function nearest_facilities(double precision,double precision,int) is 'Returns nearest active facilities to a given latitude/longitude (meters).';

-- Check if facility lies within service area
create or replace function is_facility_in_service_area(p_facility_id int, p_service_area_id int)
returns boolean as $$
declare v_result boolean; begin
    select ST_Contains(sa."Geom", f."Geom") into v_result
    from "ServiceAreas" sa join "Facilities" f on sa."Id"=p_service_area_id and f."Id"=p_facility_id
    where sa."Geom" is not null and f."Geom" is not null;
    return coalesce(v_result,false);
end; $$ language plpgsql stable;

comment on function is_facility_in_service_area(int,int) is 'True when facility geometry is contained within service area geometry.';

-- Service areas covering a location
create or replace function service_areas_for_location(p_lat double precision, p_lon double precision)
returns table("ServiceAreaId" int, "Name" text, "Type" text) as $$
begin
    return query
    select sa."Id", sa."Name", sa."Type"
    from "ServiceAreas" sa
    where sa."Geom" is not null
      and ST_Contains(sa."Geom", ST_SetSRID(ST_MakePoint(p_lon,p_lat),4326));
end; $$ language plpgsql stable;

comment on function service_areas_for_location(double precision,double precision) is 'Lists service areas whose geometry contains the provided location.';

-- Distance between two patients (based on primary address lat/long) - demonstration
create or replace function patient_distance_km(p_patient_a int, p_patient_b int)
returns double precision as $$
declare lat1 numeric; lon1 numeric; lat2 numeric; lon2 numeric; begin
    select a."Latitude", a."Longitude" into lat1, lon1
      from "PatientAddresses" pa join "Addresses" a on pa."AddressId"=a."Id"
      where pa."PatientId"=p_patient_a and pa."IsPrimary"=true limit 1;
    select a."Latitude", a."Longitude" into lat2, lon2
      from "PatientAddresses" pa join "Addresses" a on pa."AddressId"=a."Id"
      where pa."PatientId"=p_patient_b and pa."IsPrimary"=true limit 1;
    if lat1 is null or lat2 is null then return null; end if;
    return ST_DistanceSphere(ST_MakePoint(lon1,lat1), ST_MakePoint(lon2,lat2)) / 1000.0; -- km
end; $$ language plpgsql stable;

comment on function patient_distance_km(int,int) is 'Approx great-circle distance (km) between two patients primary addresses.';

-- K5. Spatial Indexes & Views --------------------------------------------------
do $$ begin
    begin
        create index if not exists "GIX_Facilities_Geom" on "Facilities" using gist ("Geom");
        create index if not exists "GIX_ServiceAreas_Geom" on "ServiceAreas" using gist ("Geom");
    exception when undefined_table then null; end;
end $$;

create or replace view "FacilityCoverage" as
    select f."Id" as "FacilityId", f."Name" as "FacilityName", sa."Id" as "ServiceAreaId", sa."Name" as "ServiceAreaName",
           is_facility_in_service_area(f."Id", sa."Id") as "Inside"
    from "Facilities" f cross join "ServiceAreas" sa
    where f."Geom" is not null and sa."Geom" is not null;

comment on view "FacilityCoverage" is 'Cartesian coverage view (use filter on Inside=true) for facility to service area containment.';

-- K6. Sync Triggers ------------------------------------------------------------
-- When Facilities.Coordinates changes, recompute Geom / Geog
create or replace function trg_facilities_sync_geometry() returns trigger as $$
begin
    if NEW."Coordinates" is not null then
        NEW."Geom" := ST_SetSRID(ST_MakePoint( (NEW."Coordinates"[0])::float8, (NEW."Coordinates"[1])::float8 ),4326);
        NEW."Geog" := ST_GeographyFromText(format('SRID=4326;POINT(%s %s)', (NEW."Coordinates"[0])::float8, (NEW."Coordinates"[1])::float8));
    end if;
    return NEW;
exception when undefined_function then
    return NEW; -- PostGIS not installed
end; $$ language plpgsql;

drop trigger if exists facilities_sync_geometry on "Facilities";
create trigger facilities_sync_geometry before insert or update of "Coordinates" on "Facilities" for each row execute function trg_facilities_sync_geometry();

-- When ServiceAreas.Definition (GeoJSON) changes, recompute Geom/Geog
create or replace function trg_serviceareas_sync_geometry() returns trigger as $$
begin
    if NEW."Definition" is not null and NEW."Definition" ? 'type' then
        NEW."Geom" := ST_SetSRID(ST_GeomFromGeoJSON(NEW."Definition"::text),4326);
        NEW."Geog" := ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(NEW."Definition"::text),4326),4326)::geography;
    end if;
    return NEW;
exception when undefined_function then
    return NEW;
end; $$ language plpgsql;

drop trigger if exists serviceareas_sync_geometry on "ServiceAreas";
create trigger serviceareas_sync_geometry before insert or update of "Definition" on "ServiceAreas" for each row execute function trg_serviceareas_sync_geometry();

-- Patch Set K complete.

