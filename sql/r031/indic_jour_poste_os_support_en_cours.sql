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
	kvhrb.alm_asset_asset_tag_display_value,
	DATE_PART('day', now()- b.lastscan ) day_last_seen,
	viws.summarization,  
	nni.direction,
	nni.division,
	nni.regroupement_unite,
	nni.um,
	nni.dum,
	nni.code_moa
from use_case_specif.vue_hok_r031_base kvhrb
left join use_case_specif.vue_lastscan b on kvhrb.alm_asset_asset_tag_display_value = b.hostname
left join use_case_specif.vue_imi_wsu_hok_s12_etat_poste viws on kvhrb.alm_asset_asset_tag_display_value = viws.substring
left join utilisateurs nni on kvhrb.user_user_name_display_value = nni."Matricule__NNI_"
where kvhrb."SupportEndDateAsDate">now()::date and kvhrb."SupportEndDateAsDate" notnull and viws.summarization =7
 ) 
select
	'Nb postes os supporté en cours' as indicateur,
	'Nb postes os supporté' as indicateur_parent,
	concat(direction, '/' , division , '/' , regroupement_unite , '/' , um , '/' , dum , '/' , code_moa) maille,
	'groupes mailles direction/division/regroupement_unite/um/dum/code rc' as maille_parent,
	count(f.alm_asset_asset_tag_display_value) as valeur
from
	itam f
	where f.day_last_seen<31 and f.day_last_seen notnull
group by
	concat(direction, '/' , division , '/' , regroupement_unite , '/' , um , '/' , dum , '/' , code_moa)
