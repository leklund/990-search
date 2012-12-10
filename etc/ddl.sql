/* SQL SQL SQL */
/* UNTZ UNTZ UNTZ*/

begin;

drop schema if exists npo cascade;

create schema npo;

--tables
create table npo.files(
  id serial primary key,
  name text,
  base_name text,
  imported boolean not null default false,
  hash text
);

create table npo.irs_raw(
  id serial primary key,
  file_id integer not null,
  ein integer not null,
  filing_period text,
  taxpayer_name text,
  state text,
  zip text,
  return_type text,
  subsection_code text,
  total_assets float,
  scan_date date,
  created_at timestamp with time zone not null default now()
);

create table npo.orgs (
  id serial primary key,
  ein integer not null unique,
  name text,
  state text,
  zip text,
  ts_index_col tsvector,
  created_at timestamp with time zone not null default now(),
  updated_at timestamp with time zone not null default now()
);

create table npo.org_aliases (
  org_id integer,
  alias text
);

create table npo.filings (
  id serial primary key,
  filing_date date not null,
  org_id integer not null,
  irs_raw_id integer not null,
  total_assets float,
  pdf_path text,
  form_type text,
  created_at timestamp with time zone not null default now()
);

--indexes and foreign keys
create index files_name_idx on npo.files(name);
create index irs_raw_file_id_idx on npo.irs_raw(file_id);
create index orgs_name_idx on npo.orgs(name);
create index orgs_state_idx on npo.orgs(state);
create index orgs_zip_idx on npo.orgs(zip);
create index orgs_ts_idx on npo.orgs using gin(ts_index_col);
create index filings_org_id_idx on npo.filings(org_id);

alter table npo.filings add foreign key (irs_raw_id) references npo.irs_raw(id);
alter table npo.irs_raw add foreign key (file_id) references npo.files;
alter table npo.filings add foreign key (org_id) references npo.orgs;
alter table npo.org_aliases add foreign key (org_id) references npo.orgs;

-- functions and triggers
create function npo.make_path_name(_file_id integer, _ein integer, _type text, _filing text) returns text as
$$
  select '/irs.gov/eo/' || d.base_name || '/' || regexp_replace($2::text, '(^\d{2})' ,'\1-') || '_' || $3 || '_' || $4 || '.pdf' as path from
  (select base_name from npo.files where id = $1) d;
$$
language sql stable;


create function npo.post_import_work() RETURNS trigger AS $$
declare
  _org_id integer;
  _ein integer;
  raw npo.irs_raw%rowtype;
begin
  _ein := 0;
  for raw in (select * from npo.irs_raw where file_id = new.id) loop
    if _ein = 0 or _ein != raw.ein then
      select find_or_create_org(raw.ein, raw.taxpayer_name, raw.state, raw.zip) into _org_id;
      insert into npo.filings
        (filing_date, org_id, irs_raw_id, total_assets, pdf_path, form_type)
        values
        ((raw.filing_period || '01')::date, _org_id, raw.id, raw.total_assets, make_path_name(raw.file_id, raw.ein, raw.return_type, raw.filing_period), raw.return_type);
    end if;
  end loop;
  return new;
end;
$$
language plpgsql;


create trigger files_updated AFTER UPDATE ON npo.files
for each row
  when (old.imported is false and new.imported is true)
  execute procedure npo.post_import_work();

create function npo.find_or_create_org(in_ein integer, _name text, state text, zip text) RETURNS integer as $$
  WITH v AS (SELECT $1 AS ein, $2 as name, $3 as state, $4 as zip),
       s AS (SELECT id FROM npo.orgs JOIN v USING (ein)),
       i AS (
         INSERT INTO npo.orgs (ein, name, state, zip)
         (SELECT ein, name, state, zip
         FROM   v
         WHERE  NOT EXISTS (SELECT * FROM s))
         RETURNING id
         )
  SELECT id FROM i
  UNION  ALL
  SELECT id FROM s;
$$
language sql volatile;

create function npo.make_ts_column() returns trigger as $$
begin
  if TG_OP = 'INSERT' then
    new.ts_index_col := to_tsvector('english', new.name);
    return new;
  elsif TG_OP = 'UPDATE' then
    if new.name != old.name then
      -- move the name to aliases table and update the ts_vector
      insert into npo.org_aliases (org_id, alias) select old.id, old.name
      WHERE NOT exists (select org_id from npo.org_aliases where alias = old.name);
      new.ts_index_col := to_tsvector('english', new.name || ' ' || (select array_to_string(array(select alias from npo.org_aliases where org_id = new.id), ' ')));
    end if;
    return new;
  end if;
end;
$$
language plpgsql;

create trigger org_updated BEFORE INSERT OR UPDATE ON npo.orgs for each row execute procedure npo.make_ts_column();

-- dummy
/*
insert into npo.files (name, base_name) values ('irs.2002_01_PF.dat.txt', '2002_01_PF');
insert into npo.irs_raw (file_id, ein, filing_period, taxpayer_name, state, zip, return_type, total_assets) VALUES (1, 521776572,'201210','LUKASCHUCK','CO','80907', '990',789564782);
*/
commit;
