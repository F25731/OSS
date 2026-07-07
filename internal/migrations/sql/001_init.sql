create table if not exists images (
	id text primary key,
	public_path text not null unique,
	file_path text not null unique,
	original_name text not null,
	size_bytes bigint not null,
	mime_type text not null,
	sha256 text not null,
	api_key_id text,
	api_key_name text not null,
	status text not null default 'active',
	delete_error text not null default '',
	delete_attempts int not null default 0,
	created_at timestamptz not null default now(),
	updated_at timestamptz not null default now(),
	deleted_at timestamptz
);

alter table images add column if not exists api_key_id text;
alter table images add column if not exists status text;
alter table images add column if not exists delete_error text not null default '';
alter table images add column if not exists delete_attempts int not null default 0;
alter table images add column if not exists updated_at timestamptz not null default now();
update images
set status = case when deleted_at is null then 'active' else 'deleted' end
where status is null or status = '';
alter table images alter column status set default 'active';
alter table images alter column status set not null;

create index if not exists images_active_created_at_idx on images (created_at) where deleted_at is null;
create index if not exists images_visible_created_at_idx on images (created_at desc) where status in ('active', 'delete_failed');
create index if not exists images_cleanup_created_at_idx on images (created_at asc) where status = 'active';
create index if not exists images_deleted_at_idx on images (deleted_at asc) where status = 'deleted';
create index if not exists images_sha256_idx on images (sha256);
create index if not exists images_api_key_id_idx on images (api_key_id) where deleted_at is null;
create index if not exists images_visible_api_key_created_idx on images (api_key_id, created_at desc) where status in ('active', 'delete_failed');

create table if not exists api_keys (
	id text primary key,
	name text not null,
	key_hash text not null unique,
	prefix text not null,
	enabled boolean not null default true,
	created_at timestamptz not null default now(),
	last_used_at timestamptz
);
create index if not exists api_keys_enabled_idx on api_keys (enabled);

create table if not exists upload_logs (
	id bigserial primary key,
	image_id text,
	api_key_id text,
	api_key_name text not null default '',
	original_name text not null default '',
	size_bytes bigint not null default 0,
	mime_type text not null default '',
	ip text not null default '',
	user_agent text not null default '',
	status text not null,
	message text not null default '',
	created_at timestamptz not null default now()
);
create index if not exists upload_logs_created_at_idx on upload_logs (created_at desc);
create index if not exists upload_logs_api_key_id_idx on upload_logs (api_key_id);

create table if not exists storage_stats (
	id int primary key check (id = 1),
	image_count bigint not null default 0,
	total_bytes bigint not null default 0,
	updated_at timestamptz not null default now()
);
insert into storage_stats (id, image_count, total_bytes) values (1, 0, 0) on conflict (id) do nothing;
update storage_stats
set image_count = coalesce((select count(*) from images where status in ('active', 'delete_failed')), 0),
	total_bytes = coalesce((select sum(size_bytes) from images where status in ('active', 'delete_failed')), 0),
	updated_at = now()
where id = 1;

create table if not exists storage_events (
	id bigserial primary key,
	delta_count bigint not null,
	delta_bytes bigint not null,
	created_at timestamptz not null default now()
);
create index if not exists storage_events_id_idx on storage_events (id);

create table if not exists settings (
	key text primary key,
	value text not null
);

insert into settings (key, value) values
	('retention_days', '7'),
	('capacity_gb', '100'),
	('trim_gb', '30'),
	('cleanup_interval_minutes', '10'),
	('cleanup_batch_size', '1000'),
	('log_retention_days', '30'),
	('deleted_record_retention_days', '7')
on conflict (key) do nothing;
