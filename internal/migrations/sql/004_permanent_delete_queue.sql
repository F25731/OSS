create index if not exists images_permanent_delete_queue_idx
	on images (updated_at asc, id asc)
	where retention_policy = 'permanent' and status in ('delete_queued', 'delete_failed');

create index if not exists images_permanent_deleting_idx
	on images (updated_at asc)
	where retention_policy = 'permanent' and status = 'deleting';
