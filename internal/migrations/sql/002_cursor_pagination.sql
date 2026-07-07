create index if not exists images_visible_created_id_idx
	on images (created_at desc, id desc)
	where status in ('active', 'delete_failed');
