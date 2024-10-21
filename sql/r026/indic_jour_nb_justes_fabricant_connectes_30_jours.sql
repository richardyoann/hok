-- CTE pour lister les utilisateurs
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
			coalesce((regexp_match(nni.direction, '.*\((.*)\)'))[1], '') as direction,
			coalesce((regexp_match(nni.division, '.*\((.*)\)'))[1], '') as division,
			coalesce((regexp_match(nni.regroupement_unite, '.*\((.*)\)'))[1], '') as regroupement_unite,
			coalesce((regexp_match(nni.um, '.*\((.*)\)'))[1], '') as um,
			coalesce((regexp_match(nni.dum, '.*\((.*)\)'))[1], '') as dum,
			coalesce((regexp_match(nni.code_moa, '.*\((.*)\)'))[1], '') as code_moa
		from use_case_specif.ref_utilisateurs nni
	union all
		select
			nni."Matricule__NNI_" ,
			coalesce((regexp_match(nni.direction, '.*\((.*)\)'))[1], '') as direction,
			coalesce((regexp_match(nni.division, '.*\((.*)\)'))[1], '') as division,
			coalesce((regexp_match(nni.regroupement_unite, '.*\((.*)\)'))[1], '') as regroupement_unite,
			coalesce((regexp_match(nni.um, '.*\((.*)\)'))[1], '') as um,
			coalesce((regexp_match(nni.dum, '.*\((.*)\)'))[1], '') as dum,
			coalesce((regexp_match(nni.code_moa, '.*\((.*)\)'))[1], '') as code_moa
		from use_case_specif.ref_departements nni 
	) nni
	where
		"Matricule__NNI_" is not null
		and trim("Matricule__NNI_") <> ''
),
-- CTE pour lister les postes MECM et les données permettant de calculer la justesse
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
		"User_Last_connexion" ,
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
			when replace(replace ( replace("ListMacaddress"::text, ',', '/'), '{', ''), '}', '') like '%NULL%' then null
			else replace(replace ( replace("ListMacaddress"::text, ',', '/'), '{', ''), '}', '')
		end as "ListMacaddress"
	from use_case_specif.vue_s12_workstations_infos
	order by
		"Workstation" ,
		"Last_Scan" desc 
),
-- CTE pour lister les postes se trouvant dans itam et vu dans le parc durant les 30 derniers jours 
itam as (
	select
		distinct on
		(trim(upper(win.alm_asset_asset_tag_display_value))) 
		trim(upper(win.alm_asset_asset_tag_display_value)) as alm_asset_asset_tag_display_value ,
		win.cmdb_model_manufacturer_display_value,
		uti.um as uti_um,
		uti.dum as uti_dum,
		uti.direction,
		uti.division,
		uti.regroupement_unite,
		uti.code_moa,
		COALESCE(win.cmdb_ci_actif_u_network_access_display_value,''::TEXT) cmdb_ci_actif_u_network_access_display_value
	from use_case_specif.vue_imi_dcw_postes_win win
	left join utilisateurs uti 
		on upper(uti."Matricule__NNI_") = upper(win.nni)
	LEFT JOIN use_case_specif.vue_lastscan s 
		ON s.hostname = win.alm_asset_asset_tag_display_value	
	WHERE (current_date ::date - s.lastscan ::date ) ::integer < 31		
	order by
		trim(upper(win.alm_asset_asset_tag_display_value)) ,
		win.alm_asset_sys_updated_on_display_value desc 
),
-- CTE premettant le calcul de la justesse
justesse as (
	select
		i.alm_asset_asset_tag_display_value,
		direction,
		division,
		regroupement_unite,
		uti_um,
		uti_dum,
		code_moa,
		case
			when mecm."Workstation_Manufacturer" is null
				or cmdb_model_manufacturer_display_value is null
				or trim(mecm."Workstation_Manufacturer" ) = ''
				or trim(cmdb_model_manufacturer_display_value) = '' 
			then 3
			when trim(upper(mecm."Workstation_Manufacturer")) = trim(upper(cmdb_model_manufacturer_display_value)) then 1
			else 2
		end as j_fabricant
	from itam i
	left join mecm 
		on	mecm."Workstation" = i.alm_asset_asset_tag_display_value
	WHERE i.cmdb_ci_actif_u_network_access_display_value <> 'Hors RLE'::TEXT
) 
--Requête finale
select
	'Nb postes fabricant connectes juste 30 js' as indicateur,
	'Nb postes' as indicateur_parent,
	concat(direction, '/' , division , '/' , regroupement_unite , '/' , uti_um , '/' , uti_dum , '/' , code_moa) maille,
	'groupes mailles direction/division/regroupement_unite/um/dum/code rc' as maille_parent,
	sum(case when j_fabricant = 1 then 1 else 0 end ) as valeur
from justesse f
group by concat(direction, '/' , division , '/' , regroupement_unite , '/' , uti_um , '/' , uti_dum , '/' , code_moa)