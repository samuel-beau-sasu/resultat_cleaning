// definitions/generate_predictions.js

// Définir les indices sur lesquels nous voulons itérer
const indices = ["DJI"];

// Définir le nombre de paires de prédictions (y_proba_n, y_pred_n)
const numPredictions = 10;

// Boucler sur chaque indice
indices.forEach(index => {
  // Boucler sur chaque paire de prédictions (de 1 à 10)
  for (let i = 1; i <= numPredictions; i++) {
    // Définir le nom de la table pour cette combinaison
    const tableName = `results_agg_${index}_pred_${i}`;

    // Générer le SQLX pour chaque table
    publish(tableName,{
      type: "table", // <-- AJOUTEZ CETTE LIGNE ICI
      description: `Table de prédiction ${i} pour l'indice ${index}.` // Optionnel : ajoute une description dynamique
    }).query(ctx => `
      with resutat_${index}_${i} as (
        SELECT
                Date,
                Date_ref,
                step,
                y_proba_${i},
                y_pred_${i},
                upload_timestamp,
                row_number() OVER(PARTITION BY Date, Date_ref ORDER BY upload_timestamp DESC) AS rn
              FROM \`financial-data-storage.prevision_prod.results_agregation_${index}\`
              ORDER BY Date DESC
      )
    , resultat_hl_${index}_${i} as (
        SELECT
              Date,
              Date_ref,
              step,
              y_proba_${i} as y_proba_hl_${i},
              y_pred_${i} as y_pred_hl_${i},
              upload_timestamp,
              row_number() OVER(PARTITION BY Date, Date_ref ORDER BY upload_timestamp DESC) AS rn
            FROM \`financial-data-storage.prevision_prod.results_hl_agregation_${index}\`
            ORDER BY Date DESC
    )

      select 
                a.Date,
                a.Date_ref,
                a.step,
                a.y_proba_${i},
                a.y_pred_${i},
                b.y_proba_hl_${i},
                b.y_pred_hl_${i},
                a.upload_timestamp
      from resutat_${index}_${i} as a 
      left join resultat_hl_${index}_${i} as b
      on a.Date = b.Date
      and a.Date_ref = b.Date_ref
      and a.step = b.step
      and a.rn = b.rn
      where a.rn = 1
      order by a.Date_ref desc, a.Date desc
    `);
  }
});