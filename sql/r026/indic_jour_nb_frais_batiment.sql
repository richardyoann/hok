with utilisateurs as (
select
	distinct
	nni."Matricule__NNI_" ,
	nni.direction,
	nni.division,
	nni.regroupement_unite,
	nni.um,
	nni.dum,
	nni.code_moa
from
	(
	select
		nni."Matricule__NNI_" ,
		coalesce((regexp_match(nni.direction, '.*\((.*)\)'))[1], '') direction,
		coalesce((regexp_match(nni.division, '.*\((.*)\)'))[1], '') division,
		coalesce((regexp_match(nni.regroupement_unite, '.*\((.*)\)'))[1], '') regroupement_unite,
		coalesce((regexp_match(nni.um, '.*\((.*)\)'))[1], '') um,
		coalesce((regexp_match(nni.dum, '.*\((.*)\)'))[1], '') dum,
		coalesce((regexp_match(nni.code_moa, '.*\((.*)\)'))[1], '') code_moa
	from
		use_case_specif.ref_utilisateurs nni
union all
	select
		nni."Matricule__NNI_" ,
		coalesce((regexp_match(nni.direction, '.*\((.*)\)'))[1], '') direction,
		coalesce((regexp_match(nni.division, '.*\((.*)\)'))[1], '') division,
		coalesce((regexp_match(nni.regroupement_unite, '.*\((.*)\)'))[1], '') regroupement_unite,
		coalesce((regexp_match(nni.um, '.*\((.*)\)'))[1], '') um,
		coalesce((regexp_match(nni.dum, '.*\((.*)\)'))[1], '') dum,
		coalesce((regexp_match(nni.code_moa, '.*\((.*)\)'))[1], '') code_moa
	from
		use_case_specif.ref_departements nni 
) nni
where
	"Matricule__NNI_" is not null
	and trim("Matricule__NNI_") <> ''
),
itam as (
select 
	trim(upper(win.alm_asset_asset_tag_display_value)) as alm_asset_asset_tag_display_value,
	uti.um as uti_um,
	uti.dum as uti_dum,
	uti.direction,
	uti.division,
	uti.regroupement_unite,
	uti.code_moa,
	b.justesse_batiment,
	case
		when s.lastscan is null then 'jamais vu'
		else 
	case
			when (current_date ::date - s.lastscan ::date ) ::integer < 8 then 'vu'
			else 'pas vu'
		end
	end as vu
from
	use_case_specif.vue_imi_dcw_postes_win win
left join use_case_specif.vue_lastscan s on
	s.hostname = win.alm_asset_asset_tag_display_value
left join utilisateurs uti on
	upper(uti."Matricule__NNI_") = upper(win.nni)
left join use_case_specif.vue_nc1_imi_justesse_localisation_batiment_postes_win_parc b
on
	win.alm_asset_asset_tag_display_value = b.alm_asset_asset_tag_display_value 
 ) 
select
	'Nb postes batiment frais' as indicateur,
	'Nb postes' as indicateur_parent,
	concat(direction, '/' , division , '/' , regroupement_unite , '/' , uti_um , '/' , uti_dum , '/' , code_moa) maille,
	'groupes mailles direction/division/regroupement_unite/um/dum/code rc' as maille_parent,
	sum(case when justesse_batiment = 'Juste' and vu = 'vu' then 1 else 0 end ) as valeur
from
	itam f
group by
	concat(direction, '/' , division , '/' , regroupement_unite , '/' , uti_um , '/' , uti_dum , '/' , code_moa)
