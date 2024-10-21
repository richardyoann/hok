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
mecm as (
select
	distinct on
	("Workstation") "Workstation" ,
	"Workstation_Manufacturer",
	"Workstation_model",
	"Description_equipement_informatique",
	"OS",
	"Last_Scan",
	upper(split_part("User_Top_NNI", '-', 1)) as user_top_nni,
	"User_Top_last_refresh",
	"User_Last_connexion",
	case
		when upper(split_part("User_Top_NNI", '-', 1)) like '%-OP%'
			or upper(split_part("User_Top_NNI", '-', 1)) like '%AUTRE%'
				or upper(split_part("User_Top_NNI", '-', 1)) like '%-G%'
					or upper(split_part("User_Top_NNI", '-', 1)) like '%-PR%'
						or upper(split_part("User_Top_NNI", '-', 1)) like '%CSP%'
	then 'Exclu'
						else upper(split_part("User_Top_NNI", '-', 1))
					end as user_top_nni_exclu,
					case
						when "Workstation" like '%-%' then true
						else false
					end as virtuel,
					case
						when replace(replace ( replace("ListMacaddress"::text, ',', '/'), '{', ''), '}', '') like '%NULL%' 
then null
						else replace(replace ( replace("ListMacaddress"::text, ',', '/'), '{', ''), '}', '')
					end as "ListMacaddress"
				from
					use_case_specif.vue_s12_workstations_infos
				order by
					"Workstation" ,
					"Last_Scan" desc 
),
itam as (
select
	distinct on
	(trim(upper(win.alm_asset_asset_tag_display_value))) 
	trim(upper(win.alm_asset_asset_tag_display_value)) as alm_asset_asset_tag_display_value,
	uti.um as uti_um,
	uti.dum as uti_dum,
	uti.direction, 
	uti.division, 
	uti.regroupement_unite, 
	uti.code_moa, 
	win.cmdb_ci_mac_address_display_value
from
	use_case_specif.vue_imi_dcw_postes_win win
left join utilisateurs uti on
	upper(uti."Matricule__NNI_") = upper(win.nni)
order by
	trim(upper(win.alm_asset_asset_tag_display_value)) ,
	win.alm_asset_sys_updated_on_display_value desc 
),
justesse as (
select
	i.alm_asset_asset_tag_display_value ,
	direction ,
	division ,
	regroupement_unite ,
	uti_um ,
	uti_dum ,
	code_moa,
	case
		when mecm."ListMacaddress" is null
			or trim(mecm."ListMacaddress") = ''
				or cmdb_ci_mac_address_display_value is null
				or trim(cmdb_ci_mac_address_display_value) = ''
then 3
				when mecm."ListMacaddress" like concat('%', cmdb_ci_mac_address_display_value, '%') then 1
				else 2
			end as j_mac
		from
			itam i
		left join mecm on
			mecm."Workstation" = i.alm_asset_asset_tag_display_value
) 
select
	'Nb postes mac juste' as indicateur,
	'Nb postes' as indicateur_parent,
	concat(direction, '/' , division , '/' , regroupement_unite , '/' , uti_um , '/' , uti_dum , '/' , code_moa) maille,
	'groupes mailles direction/division/regroupement_unite/um/dum/code rc' as maille_parent,
	sum(case when j_mac = 1 then 1 else 0 end ) as valeur
from
	justesse f
group by
	concat(direction, '/' , division , '/' , regroupement_unite , '/' , uti_um , '/' , uti_dum , '/' , code_moa)