-- CTE pour lister les utilisateurs
WITH utilisateurs AS (
	SELECT
		DISTINCT
		nni."Matricule__NNI_" ,
		nni.direction,
		nni.division,
		nni.regroupement_unite,
		nni.um,
		nni.dum,
		nni.code_moa
	FROM
		(
			SELECT
				nni."Matricule__NNI_" ,
				coalesce((regexp_match(nni.direction, '.*\((.*)\)'))[1], '') direction,
				coalesce((regexp_match(nni.division, '.*\((.*)\)'))[1], '') division,
				coalesce((regexp_match(nni.regroupement_unite, '.*\((.*)\)'))[1], '') regroupement_unite,
				coalesce((regexp_match(nni.um, '.*\((.*)\)'))[1], '') um,
				coalesce((regexp_match(nni.dum, '.*\((.*)\)'))[1], '') dum,
				coalesce((regexp_match(nni.code_moa, '.*\((.*)\)'))[1], '') code_moa
			FROM use_case_specif.ref_utilisateurs nni
			UNION ALL
			SELECT
				nni."Matricule__NNI_" ,
				coalesce((regexp_match(nni.direction, '.*\((.*)\)'))[1], '') direction,
				coalesce((regexp_match(nni.division, '.*\((.*)\)'))[1], '') division,
				coalesce((regexp_match(nni.regroupement_unite, '.*\((.*)\)'))[1], '') regroupement_unite,
				coalesce((regexp_match(nni.um, '.*\((.*)\)'))[1], '') um,
				coalesce((regexp_match(nni.dum, '.*\((.*)\)'))[1], '') dum,
				coalesce((regexp_match(nni.code_moa, '.*\((.*)\)'))[1], '') code_moa
			FROM use_case_specif.ref_departements nni
		) nni
	WHERE "Matricule__NNI_" IS NOT NULL	AND trim("Matricule__NNI_") <> ''
),
-- CTE pour lister les postes MECM et les données permettant de calculer la fraicheur
mecm AS (
	SELECT
		DISTINCT ON
		("Workstation") "Workstation" ,
		"Workstation_Manufacturer",
		"Workstation_model",
		"Description_equipement_informatique",
		"OS",
		"Last_Scan",
		upper(split_part("User_Top_NNI", '-', 1)) AS user_top_nni,
		"User_Top_last_refresh",
		"User_Last_connexion",
		CASE
			WHEN upper(split_part("User_Top_NNI", '-', 1)) LIKE '%-OP%' OR upper(split_part("User_Top_NNI", '-', 1)) LIKE '%AUTRE%'	OR upper(split_part("User_Top_NNI", '-', 1)) LIKE '%-G%' OR upper(split_part("User_Top_NNI", '-', 1)) LIKE '%-PR%' OR upper(split_part("User_Top_NNI", '-', 1)) LIKE '%CSP%' 	THEN 'Exclu'
			ELSE upper(split_part("User_Top_NNI", '-', 1))
		END AS user_top_nni_exclu,
		CASE
			WHEN "Workstation" LIKE '%-%' THEN TRUE
			ELSE FALSE
		END AS virtuel,
		CASE
			WHEN REPLACE(REPLACE (REPLACE("ListMacaddress"::TEXT,',','/'),'{',''),'}','') LIKE '%NULL%' THEN NULL
			ELSE REPLACE(REPLACE (REPLACE("ListMacaddress"::TEXT,',','/'),'{',''),'}','')
		END AS "ListMacaddress",
		top_user.um
	FROM use_case_specif.vue_s12_workstations_infos a
	LEFT JOIN utilisateurs top_user 
	ON upper(split_part(a."User_Top_NNI", '-', 1)) = top_user."Matricule__NNI_"
	ORDER BY "Workstation" ,"Last_Scan" DESC
),
-- CTE pour lister les postes se trouvant dans itam et vu dans le parc durant les 30 derniers jours 
itam AS (
	SELECT
		DISTINCT ON	(	trim(upper(win.alm_asset_asset_tag_display_value))) 
		trim(upper(win.alm_asset_asset_tag_display_value)) AS alm_asset_asset_tag_display_value,
		p.cmdb_model_model_number_display_value ,
		win.cmdb_model_manufacturer_display_value,
		win.cmdb_ci_actif_virtual_value,
		uti.um AS uti_um,
		uti.dum AS uti_dum,
		uti.direction,
		uti.division,
		uti.regroupement_unite,
		uti.code_moa,
		COALESCE(win.cmdb_ci_actif_u_network_access_display_value,''::TEXT) cmdb_ci_actif_u_network_access_display_value,
		CASE
			WHEN s.lastscan IS NULL THEN 'jamais vu'
			ELSE 
				CASE
					WHEN (current_date ::date - s.lastscan ::date	) ::integer < 31 THEN 'vu'
					ELSE 'pas vu'
				END
		END AS vu
	FROM use_case_specif.vue_imi_dcw_postes_win win
	JOIN use_case_specif.vue_dfa_imi_postes p 
		ON	win.alm_asset_asset_tag_display_value = p.alm_asset_asset_tag_display_value
	LEFT JOIN utilisateurs uti 
		ON upper(uti."Matricule__NNI_") = upper(win.nni)
	LEFT JOIN use_case_specif.vue_lastscan s 
		ON s.hostname = win.alm_asset_asset_tag_display_value	
	WHERE (current_date ::date - s.lastscan ::date ) ::integer < 31	
	ORDER BY trim(upper(win.alm_asset_asset_tag_display_value)) , win.alm_asset_sys_updated_on_display_value DESC
),
-- CTE premettant le calcul de la fraicheur
fraicheur AS (
	SELECT
		i.alm_asset_asset_tag_display_value,
		direction,
		division,
		regroupement_unite,
		uti_um,
		uti_dum,
		code_moa,
		CASE
			WHEN mecm."Description_equipement_informatique" IS NULL OR cmdb_model_model_number_display_value IS NULL OR trim(mecm."Description_equipement_informatique" ) = '' OR trim(cmdb_model_model_number_display_value) = ''  THEN 3
			WHEN trim(upper(mecm."Description_equipement_informatique")) = trim(upper(cmdb_model_model_number_display_value)) THEN 1
			ELSE 2
		END AS j_modele ,
		CASE
			WHEN mecm."Workstation_Manufacturer" IS NULL OR cmdb_model_manufacturer_display_value IS NULL OR trim(mecm."Workstation_Manufacturer" ) = ''OR trim(cmdb_model_manufacturer_display_value) = '' THEN 3
			WHEN trim(upper(mecm."Workstation_Manufacturer")) = trim(upper(cmdb_model_manufacturer_display_value)) THEN 1
			ELSE 2
		END AS j_fabricant,
		CASE
			WHEN cmdb_ci_actif_virtual_value THEN 
				CASE
					WHEN mecm.virtuel THEN 1
					ELSE 2
				END
			ELSE 
				CASE
					 	WHEN mecm.virtuel THEN 2
						ELSE 1
				END
		END AS j_virtual ,
		CASE
			WHEN mecm.um IS NULL OR uti_um IS NULL THEN 3
			WHEN mecm.um = uti_um THEN 1
			ELSE 2
		END AS j_entite_utilisateur,
		vu
	FROM itam i
	LEFT JOIN mecm 
		ON	mecm."Workstation" = i.alm_asset_asset_tag_display_value		
	WHERE i.cmdb_ci_actif_u_network_access_display_value <> 'Hors RLE'::TEXT	
) 
--Requête finale
SELECT
	'Nb postes frais connectes 30 js' AS indicateur,
	'Nb postes' AS indicateur_parent,
	concat(direction,'/',division,'/',regroupement_unite,'/',uti_um,'/',uti_dum,'/',code_moa) maille,
	'groupes mailles direction/division/regroupement_unite/um/dum/code rc' AS maille_parent,
	sum(CASE WHEN j_modele = 1 AND j_entite_utilisateur = 1 AND j_fabricant = 1 AND j_virtual = 1 AND vu = 'vu' THEN 1 ELSE 0 END ) AS valeur
FROM fraicheur f
GROUP BY concat(direction,'/',division,'/',regroupement_unite,'/',uti_um,'/',uti_dum,'/',code_moa)