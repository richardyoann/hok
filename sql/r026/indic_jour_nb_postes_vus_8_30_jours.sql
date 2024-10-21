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
)
select
	'Nb postes vus entre 8 et 30 jours' as indicateur,
	'Nb postes' as indicateur_parent,
	concat(uti.direction, '/' , uti.division , '/' , uti.regroupement_unite , '/' , uti.um , '/' , uti.dum , '/' , uti.code_moa) maille,
	'groupes mailles direction/division/regroupement_unite/um/dum/code rc' as maille_parent,
	count( distinct win.alm_asset_asset_tag_display_value) as valeur
from
	use_case_specif.vue_imi_dcw_postes_win win
left join utilisateurs uti on
	upper(uti."Matricule__NNI_") = upper(win.nni)
join use_case_specif.vue_lastscan s on
	s.hostname = win.alm_asset_asset_tag_display_value
	and (current_date ::date - s.lastscan ::date ) ::integer between 8 and 30
group by
	concat(uti.direction, '/' , uti.division , '/' , uti.regroupement_unite , '/' , uti.um , '/' , uti.dum , '/' , uti.code_moa)