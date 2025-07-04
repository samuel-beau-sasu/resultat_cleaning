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
    publish(tableName).query(ctx => `
      WITH resultat_${index}_${i} AS (
        SELECT
          Date,
          Date_ref,
          step,
          y_proba_${i},
          y_pred_${i},
          upload_timestamp,
          row_number() OVER(PARTITION BY Date, Date_ref ORDER BY upload_timestamp DESC) AS rn
        FROM \`financial-data-storage.prevision_preprod.results_agregation_${index}\`
        ORDER BY Date DESC
      )

      SELECT
        Date,
        Date_ref,
        step,
        y_proba_${i},
        y_pred_${i}
      FROM resultat_${index}_${i}
      WHERE rn = 1
      ORDER BY Date_ref, Date DESC
    `);
  }
});