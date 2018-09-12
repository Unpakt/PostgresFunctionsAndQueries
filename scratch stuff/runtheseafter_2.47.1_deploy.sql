UPDATE move_plans set lead_state = 'closed' WHERE id in(
SELECT distinct move_plan_id FROM jobs
JOIN move_plans
 on move_plans.id = jobs.move_plan_id AND
user_state = 'reserved'
JOIN white_labels
on white_labels.id = move_plans.white_label_id
JOIN service_packages
on white_labels.service_package_id = service_packages.id
AND service_package_type_id = 1);

UPDATE move_plans set lead_state = 'cancelled' WHERE id in(
SELECT distinct move_plan_id FROM jobs
JOIN move_plans
 on move_plans.id = jobs.move_plan_id AND
user_state = 'reserved_cancelled'
JOIN white_labels
on white_labels.id = move_plans.white_label_id
JOIN service_packages
on white_labels.service_package_id = service_packages.id
AND service_package_type_id = 1);

UPDATE move_plans set lead_state = 'new_lead' WHERE id in(
SELECT distinct move_plans.id FROM move_plans JOIN white_labels
white_labels
on white_labels.id = move_plans.white_label_id
JOIN service_packages
on white_labels.service_package_id = service_packages.id
AND service_package_type_id = 1
AND lead_state is null
);