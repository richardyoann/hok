select
	id_calc,
	id_maille,
	date_trunc('month', (current_date - interval '1 month'))::date as "date",
	avg(valeur) as valeur
from
	use_case_specif.hcal_dfa_hok_jours j
join use_case_specif.hcal_dfa_hok_calc c
		using(id_calc)
where
	extract(month
from
	"date" ) = extract(month
from
	current_date - interval '1 month')
	and c.rapports @> array['R026']
group by
	id_calc,
	id_maille,
	date_trunc('month', (current_date - interval '1 month'))::date