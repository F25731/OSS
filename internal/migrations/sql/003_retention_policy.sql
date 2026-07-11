alter table api_keys add column if not exists retention_policy text not null default 'timed';
update api_keys set retention_policy = 'timed' where retention_policy is null or retention_policy = '';
do $$
begin
	if not exists (
		select 1 from pg_constraint
		where conname = 'api_keys_retention_policy_chk'
			and conrelid = 'api_keys'::regclass
	) then
		alter table api_keys add constraint api_keys_retention_policy_chk check (retention_policy in ('timed', 'permanent')) not valid;
	end if;
end $$;
alter table api_keys validate constraint api_keys_retention_policy_chk;
create index if not exists api_keys_retention_policy_idx on api_keys (retention_policy);

alter table images add column if not exists retention_policy text not null default 'timed';
alter table images add column if not exists delete_token_hash text;
alter table images add column if not exists delete_token_prefix text not null default '';
update images set retention_policy = 'timed' where retention_policy is null or retention_policy = '';
do $$
begin
	if not exists (
		select 1 from pg_constraint
		where conname = 'images_retention_policy_chk'
			and conrelid = 'images'::regclass
	) then
		alter table images add constraint images_retention_policy_chk check (retention_policy in ('timed', 'permanent')) not valid;
	end if;
end $$;
alter table images validate constraint images_retention_policy_chk;

create index if not exists images_retention_visible_created_idx
	on images (retention_policy, created_at desc, id desc)
	where status in ('active', 'delete_failed');
create index if not exists images_timed_cleanup_created_idx
	on images (created_at asc)
	where status = 'active' and retention_policy = 'timed';
create unique index if not exists images_delete_token_hash_idx
	on images (delete_token_hash)
	where delete_token_hash is not null and retention_policy = 'permanent';
