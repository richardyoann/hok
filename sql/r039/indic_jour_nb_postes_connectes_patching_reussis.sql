-- CTE pour lister les campagnes de déploiement
WITH list_campagnes AS (
    SELECT
        nom_groupe_deploiment, 
        type_deploiment, 
        date_symposium,
        -- Détermination de l'historisation en fonction du groupe de déploiement et de la date
        CASE
            WHEN nom_groupe_deploiment LIKE '%G2%' THEN 
                CASE
                    WHEN date_part('day', now() - (date_symposium + INTERVAL '56 days')) = 0 THEN 'G2 8'
                    WHEN date_part('day', now() - (date_symposium + INTERVAL '42 days')) = 0 THEN 'G2 6'
                    ELSE 'G2'
                END
            WHEN nom_groupe_deploiment LIKE '%G3%' THEN 
                CASE
                    WHEN date_part('day', now() - (date_symposium + INTERVAL '28 days')) = 0 THEN 'G3 4'
                    WHEN date_part('day', now() - (date_symposium + INTERVAL '21 days')) = 0 THEN 'G3 3'
                    ELSE 'G3'
                END
            ELSE ''
        END AS historisation,
        poste_serveur           
    FROM use_case_specif.vue_referentiel_groupe_deploiement
    -- Filtrer les postes déployés dans les 60 derniers jours
    WHERE poste_serveur = 'P' 
        AND date_symposium >= CURRENT_DATE - INTERVAL '60 days'
),
-- CTE pour lister les postes et leurs campagnes associées
postes_campagnes AS (
    SELECT
        (rdp.nom_groupe_deploiment || ' - ' || rdp.type_deploiment) AS campagne,
        computer,
        rdp.summarizationstate, 
        COALESCE(user_user_name_display_value, dept_ci_id_display_value) AS utilisateur,
        COALESCE(w.cmdb_ci_actif_u_network_access_display_value, ''::text) as cmdb_ci_actif_u_network_access_display_value,
        -- Calcul de l'historisation basé sur la date du symposium
        lc.historisation
    FROM use_case_specif.vue_wsu_s12_correctifs_rdp rdp 
    INNER JOIN use_case_specif.vue_imi_dcw_postes_vm_win w ON rdp.computer = w.alm_asset_asset_tag_display_value
    INNER JOIN list_campagnes lc ON lc.nom_groupe_deploiment = rdp.nom_groupe_deploiment 
                                  AND lc.type_deploiment = rdp.type_deploiment
    WHERE rdp.poste_serveur = 'P' 
       -- Vérifier si le poste a été scanné dans les 30 derniers jours
       AND EXISTS (
          SELECT 1
          FROM use_case_specif.vue_lastscan ls
          WHERE ls.hostname = w.alm_asset_asset_tag_display_value
            AND ls.lastscan::date >= (CURRENT_DATE - INTERVAL '30 days')
      )
),
-- CTE pour lister les utilisateurs et leurs informations organisationnelles
utilisateurs AS (
    SELECT DISTINCT
        nni."Matricule__NNI_",
        -- Extraction des informations organisationnelles à partir des chaînes de caractères
        COALESCE(SUBSTRING(direction FROM '\((.*?)\)'), '') AS direction,
        COALESCE(SUBSTRING(division FROM '\((.*?)\)'), '') AS division,
        COALESCE(SUBSTRING(regroupement_unite FROM '\((.*?)\)'), '') AS regroupement_unite,
        COALESCE(SUBSTRING(um FROM '\((.*?)\)'), '') AS um,
        COALESCE(SUBSTRING(dum FROM '\((.*?)\)'), '') AS dum,
        COALESCE(SUBSTRING(code_moa FROM '\((.*?)\)'), '') AS code_moa
    FROM (
        -- Combinaison des utilisateurs et des départements
        SELECT
            "Matricule__NNI_",
            direction,
            division,
            regroupement_unite,
            um,
            dum,
            code_moa
        FROM use_case_specif.ref_utilisateurs
        UNION ALL
        SELECT
            "Matricule__NNI_",
            direction,
            division,
            regroupement_unite,
            um,
            dum,
            code_moa
        FROM use_case_specif.ref_departements
    ) AS nni
    WHERE "Matricule__NNI_" IS NOT NULL
      AND TRIM("Matricule__NNI_") <> ''
)   
SELECT
	pc.campagne || ' - ' || pc.historisation  ||' -Poste connectés' AS indicateur,
	pc.campagne || ' - ' || pc.historisation  ||' -Total connectés' as indicateur_parent,
	concat(u.direction, '/' , u.division , '/' , u.regroupement_unite , '/' , u.um , '/' , u.dum , '/' , u.code_moa) as maille,
	'groupes mailles direction/division/regroupement_unite/um/dum/code rc' as maille_parent,
    
	COUNT(pc.computer) FILTER (WHERE pc.summarizationstate = 3) as valeur
FROM utilisateurs u
INNER JOIN postes_campagnes pc ON u."Matricule__NNI_" = pc.utilisateur 
WHERE pc.historisation NOT IN ('G3','G2')
AND pc.cmdb_ci_actif_u_network_access_display_value <> 'Hors RLE'::text 
GROUP BY concat(u.direction, '/' , u.division , '/' , u.regroupement_unite , '/' , u.um , '/' , u.dum , '/' , u.code_moa),pc.campagne,pc.historisation   