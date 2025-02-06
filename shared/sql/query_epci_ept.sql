WITH epci_ept AS (
    SELECT
        com_insee,
        com_nom,
        -- Remplacer epci_siren par ept_siren si ept_siren n'est pas NULL
        CASE
            WHEN ept_siren IS NOT NULL THEN ept_siren
            ELSE epci_siren
        END AS epci_siren,
        -- Remplacer epci_nom par ept_nom si ept_nom n'est pas NULL
        CASE
            WHEN ept_nom IS NOT NULL THEN ept_nom
            ELSE epci_nom
        END AS epci_nom,
        dep_insee, epci_interdep,
        -- Remplacer epci_naturejuridique par ept_naturejuridique si ept_naturejuridique n'est pas NULL
        CASE
            WHEN ept_naturejuridique IS NOT NULL THEN ept_naturejuridique
            ELSE epci_naturejuridique
        END AS epci_naturejuridique
    FROM
        ngeofr  -- Remplacer par le nom de ta table
)
-- Filtrer les lignes où epci_siren est NULL ou égal à 200054781
SELECT
    com_insee,
    com_nom,
    epci_siren,
    epci_nom,
    dep_insee,
    epci_interdep,
    epci_naturejuridique
FROM
    epci_ept
WHERE epci_siren IS NOT NULL AND  epci_siren != '200054781'
