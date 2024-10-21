-- CTE pour lister les utilisateurs et leurs informations organisationnelles
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
				COALESCE((regexp_match(nni.direction,'.*\((.*)\)'))[1],'') direction,
				COALESCE((regexp_match(nni.division,'.*\((.*)\)'))[1],'') division,
				COALESCE((regexp_match(nni.regroupement_unite,'.*\((.*)\)'))[1],'') regroupement_unite,
				COALESCE((regexp_match(nni.um,'.*\((.*)\)'))[1],'') um,
				COALESCE((regexp_match(nni.dum,'.*\((.*)\)'))[1],'') dum,
				COALESCE((regexp_match(nni.code_moa,'.*\((.*)\)'))[1],'') code_moa
			FROM
				use_case_specif.ref_utilisateurs nni
		UNION ALL
			SELECT
				nni."Matricule__NNI_" ,
				COALESCE((regexp_match(nni.direction,'.*\((.*)\)'))[1],'') direction,
				COALESCE((regexp_match(nni.division,'.*\((.*)\)'))[1],'') division,
				COALESCE((regexp_match(nni.regroupement_unite,'.*\((.*)\)'))[1],'') regroupement_unite,
				COALESCE((regexp_match(nni.um,'.*\((.*)\)'))[1],'') um,
				COALESCE((regexp_match(nni.dum,'.*\((.*)\)'))[1],'') dum,
				COALESCE((regexp_match(nni.code_moa,'.*\((.*)\)'))[1],'') code_moa
			FROM
				use_case_specif.ref_departements nni
		) nni
	WHERE "Matricule__NNI_" IS NOT NULL
	AND trim("Matricule__NNI_") <> ''
)
-- Requête principale pour obtenir les statistiques de déploiement
SELECT
	'Nb postes connectes 30 js' AS indicateur,
	'Nb postes' AS indicateur_parent,
	concat(	uti.direction,'/' ,uti.division ,'/' ,uti.regroupement_unite ,'/' ,uti.um ,'/' ,uti.dum ,'/' ,uti.code_moa) maille,
	'groupes mailles direction/division/regroupement_unite/um/dum/code rc' AS maille_parent,
	count( DISTINCT win.alm_asset_asset_tag_display_value) AS valeur
FROM use_case_specif.vue_imi_dcw_postes_win win
LEFT JOIN utilisateurs uti 
	ON	upper(uti."Matricule__NNI_") = upper(win.nni)
LEFT JOIN use_case_specif.vue_lastscan s 
	ON s.hostname = win.alm_asset_asset_tag_display_value	
-- Filtre permettant de ne prendre que les postes connectés sous 30 jours 
WHERE (current_date ::date - s.lastscan ::date ) ::integer < 31
-- Filtre permettant de ne prendre que les postes connectés 
AND COALESCE(win.cmdb_ci_actif_u_network_access_display_value,''::TEXT) <> 'Hors RLE'::TEXT
GROUP BY concat(	uti.direction,'/' ,uti.division ,'/' ,uti.regroupement_unite ,'/' ,uti.um ,'/' ,uti.dum ,'/' ,uti.code_moa);