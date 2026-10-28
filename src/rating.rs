use std::collections::HashMap;

pub const ELO_SCALE: f64 = 400.0 / std::f64::consts::LN_10;

#[derive(Debug, Clone, Copy)]
pub struct PairCounts {
    pub lo: u16,
    pub hi: u16,
    pub games: f64,
    pub lo_wins: f64,
    pub lo_radiant_games: f64,
    pub lo_radiant_wins: f64,
}

#[derive(Debug, Clone, Copy)]
pub struct HeroCounts {
    pub hero_id: u16,
    pub games: u64,
    pub wins: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct FitOptions {
    pub lambda: f64,
    pub gamma_prior: f64,
    pub max_iterations: usize,
}

impl Default for FitOptions {
    fn default() -> Self {
        Self {
            lambda: 1.0,
            gamma_prior: 1.0,
            max_iterations: 50,
        }
    }
}

#[derive(Debug, Clone)]
pub struct HeroRating {
    pub hero_id: u16,
    pub rating: f64,
    pub rating_se: f64,
    pub games: u64,
    pub wins: u64,
    pub win_rate: f64,
}

#[derive(Debug, Clone, Default)]
pub struct FitResult {
    pub ratings: Vec<HeroRating>,
    pub radiant_advantage: f64,
    pub iterations: usize,
    pub converged: bool,
    pub log_likelihood: f64,
}

#[derive(Debug, Clone, Copy)]
struct Observation {
    winner: usize,
    loser: usize,
    count: f64,
    /// Side of the winner: +1 radiant, -1 dire.
    side: f64,
}

/// Fits a Bradley–Terry model over cross-team hero pairs, with a radiant
/// advantage term and an L2 prior. Each match contributes 25 pair outcomes.
pub fn fit(pairs: &[PairCounts], heroes: &[HeroCounts], options: FitOptions) -> FitResult {
    let mut hero_ids: Vec<u16> = heroes
        .iter()
        .map(|h| h.hero_id)
        .chain(pairs.iter().flat_map(|p| [p.lo, p.hi]))
        .collect();
    hero_ids.sort_unstable();
    hero_ids.dedup();
    if hero_ids.is_empty() {
        return FitResult {
            converged: true,
            ..FitResult::default()
        };
    }

    let hero_index: HashMap<u16, usize> = hero_ids
        .iter()
        .enumerate()
        .map(|(index, hero)| (*hero, index))
        .collect();
    let hero_count = hero_ids.len();
    let gamma = hero_count;
    let params = hero_count + 1;

    let observations = build_observations(pairs, &hero_index);
    if observations.is_empty() {
        return FitResult {
            ratings: hero_ids
                .iter()
                .map(|hero_id| {
                    let counts = hero_counts(heroes, *hero_id);
                    HeroRating {
                        hero_id: *hero_id,
                        rating: 1500.0,
                        rating_se: 0.0,
                        games: counts.0,
                        wins: counts.1,
                        win_rate: ratio(counts.1, counts.0),
                    }
                })
                .collect(),
            radiant_advantage: 0.0,
            iterations: 0,
            converged: true,
            log_likelihood: 0.0,
        };
    }

    let mut theta = vec![0.0; params];
    let mut converged = false;
    let mut iterations = 0;

    for iteration in 1..=options.max_iterations {
        iterations = iteration;
        let (mut gradient, mut hessian, _) = gradient_hessian(&theta, &observations, params);

        for i in 0..hero_count {
            gradient[i] -= options.lambda * theta[i];
            hessian[i * params + i] += options.lambda;
        }
        gradient[gamma] -= options.gamma_prior * theta[gamma];
        hessian[gamma * params + gamma] += options.gamma_prior;

        let Some(delta) = solve(&hessian, &gradient, params) else {
            break;
        };

        let current = posterior(&theta, &observations, &options, params);
        let mut step = 1.0;
        let mut candidate = theta.clone();
        let mut accepted = false;
        for _ in 0..25 {
            for i in 0..params {
                candidate[i] = theta[i] + step * delta[i];
            }
            let next = posterior(&candidate, &observations, &options, params);
            if next >= current - 1e-10 {
                accepted = true;
                break;
            }
            step *= 0.5;
        }
        if !accepted {
            // The line search could not improve the posterior. This is a
            // stalled optimization, not convergence.
            converged = false;
            break;
        }

        let max_change = candidate
            .iter()
            .zip(theta.iter())
            .map(|(a, b)| (a - b).abs())
            .fold(0.0, f64::max);
        theta = candidate;
        if max_change < 1e-9 {
            converged = true;
            break;
        }
    }

    let (_, mut hessian, data_log_likelihood) = gradient_hessian(&theta, &observations, params);
    for i in 0..hero_count {
        hessian[i * params + i] += options.lambda;
    }
    hessian[gamma * params + gamma] += options.gamma_prior;
    let inverse = invert(&hessian, params);

    let ratings = hero_ids
        .iter()
        .enumerate()
        .map(|(index, hero_id)| {
            let counts = hero_counts(heroes, *hero_id);
            let se = inverse
                .as_ref()
                .map(|inv| inv[index * params + index].max(0.0).sqrt() * ELO_SCALE)
                .unwrap_or(0.0);
            HeroRating {
                hero_id: *hero_id,
                rating: 1500.0 + ELO_SCALE * theta[index],
                rating_se: se,
                games: counts.0,
                wins: counts.1,
                win_rate: ratio(counts.1, counts.0),
            }
        })
        .collect();

    FitResult {
        ratings,
        radiant_advantage: ELO_SCALE * theta[gamma],
        iterations,
        converged,
        log_likelihood: data_log_likelihood,
    }
}

fn build_observations(pairs: &[PairCounts], hero_index: &HashMap<u16, usize>) -> Vec<Observation> {
    let mut observations = Vec::with_capacity(pairs.len() * 4);
    for pair in pairs {
        let Some(&lo) = hero_index.get(&pair.lo) else {
            continue;
        };
        let Some(&hi) = hero_index.get(&pair.hi) else {
            continue;
        };
        if lo == hi {
            continue;
        }
        let games = pair.games.max(0.0);
        let lo_wins = pair.lo_wins.clamp(0.0, games);
        let lo_radiant_games = pair.lo_radiant_games.clamp(0.0, games);
        let lo_radiant_wins = pair
            .lo_radiant_wins
            .clamp(0.0, lo_radiant_games)
            .min(lo_wins);
        let lo_dire_games = games - lo_radiant_games;
        let lo_dire_wins = lo_wins - lo_radiant_wins;

        push_observation(&mut observations, lo, hi, lo_radiant_wins, 1.0);
        push_observation(
            &mut observations,
            hi,
            lo,
            lo_radiant_games - lo_radiant_wins,
            -1.0,
        );
        push_observation(&mut observations, lo, hi, lo_dire_wins, -1.0);
        push_observation(&mut observations, hi, lo, lo_dire_games - lo_dire_wins, 1.0);
    }
    observations
}

fn push_observation(
    observations: &mut Vec<Observation>,
    winner: usize,
    loser: usize,
    count: f64,
    side: f64,
) {
    if count > 1e-9 {
        observations.push(Observation {
            winner,
            loser,
            count,
            side,
        });
    }
}

fn gradient_hessian(
    theta: &[f64],
    observations: &[Observation],
    params: usize,
) -> (Vec<f64>, Vec<f64>, f64) {
    let gamma = params - 1;
    let mut gradient = vec![0.0; params];
    let mut hessian = vec![0.0; params * params];
    let mut log_likelihood = 0.0;

    for observation in observations {
        let eta =
            theta[observation.winner] - theta[observation.loser] + theta[gamma] * observation.side;
        let p = sigmoid(eta);
        let residual = observation.count * (1.0 - p);
        gradient[observation.winner] += residual;
        gradient[observation.loser] -= residual;
        gradient[gamma] += residual * observation.side;

        let weight = observation.count * p * (1.0 - p);
        add_hessian(
            &mut hessian,
            params,
            observation.winner,
            observation.winner,
            weight,
        );
        add_hessian(
            &mut hessian,
            params,
            observation.loser,
            observation.loser,
            weight,
        );
        add_hessian(
            &mut hessian,
            params,
            observation.winner,
            observation.loser,
            -weight,
        );
        add_hessian(
            &mut hessian,
            params,
            observation.loser,
            observation.winner,
            -weight,
        );
        add_hessian(
            &mut hessian,
            params,
            observation.winner,
            gamma,
            weight * observation.side,
        );
        add_hessian(
            &mut hessian,
            params,
            gamma,
            observation.winner,
            weight * observation.side,
        );
        add_hessian(
            &mut hessian,
            params,
            observation.loser,
            gamma,
            -weight * observation.side,
        );
        add_hessian(
            &mut hessian,
            params,
            gamma,
            observation.loser,
            -weight * observation.side,
        );
        add_hessian(&mut hessian, params, gamma, gamma, weight);

        log_likelihood += observation.count * ln_sigmoid(eta);
    }

    (gradient, hessian, log_likelihood)
}

fn add_hessian(hessian: &mut [f64], params: usize, row: usize, col: usize, value: f64) {
    hessian[row * params + col] += value;
}

fn posterior(
    theta: &[f64],
    observations: &[Observation],
    options: &FitOptions,
    params: usize,
) -> f64 {
    let gamma = params - 1;
    let mut value = 0.0;
    for observation in observations {
        let eta =
            theta[observation.winner] - theta[observation.loser] + theta[gamma] * observation.side;
        value += observation.count * ln_sigmoid(eta);
    }
    for rating in &theta[..gamma] {
        value -= 0.5 * options.lambda * rating * rating;
    }
    value -= 0.5 * options.gamma_prior * theta[gamma] * theta[gamma];
    value
}

fn sigmoid(value: f64) -> f64 {
    if value >= 0.0 {
        1.0 / (1.0 + (-value).exp())
    } else {
        let exp = value.exp();
        exp / (1.0 + exp)
    }
}

fn ln_sigmoid(value: f64) -> f64 {
    -softplus(-value)
}

fn softplus(value: f64) -> f64 {
    if value > 0.0 {
        value + (-value).exp().ln_1p()
    } else {
        value.exp().ln_1p()
    }
}

fn hero_counts(heroes: &[HeroCounts], hero_id: u16) -> (u64, u64) {
    heroes
        .iter()
        .find(|h| h.hero_id == hero_id)
        .map(|h| (h.games, h.wins))
        .unwrap_or((0, 0))
}

fn ratio(wins: u64, games: u64) -> f64 {
    if games == 0 {
        0.0
    } else {
        wins as f64 / games as f64
    }
}

fn solve(matrix: &[f64], rhs: &[f64], n: usize) -> Option<Vec<f64>> {
    let mut a = matrix.to_vec();
    let mut b = rhs.to_vec();
    for col in 0..n {
        let pivot = pivot_row(&a, n, col)?;
        if pivot != col {
            swap_rows(&mut a, n, pivot, col);
            b.swap(pivot, col);
        }
        let diagonal = a[col * n + col];
        for row in (col + 1)..n {
            let factor = a[row * n + col] / diagonal;
            if factor == 0.0 {
                continue;
            }
            for k in col..n {
                a[row * n + k] -= factor * a[col * n + k];
            }
            b[row] -= factor * b[col];
        }
    }
    let mut x = vec![0.0; n];
    for row in (0..n).rev() {
        let mut sum = b[row];
        for col in (row + 1)..n {
            sum -= a[row * n + col] * x[col];
        }
        let diagonal = a[row * n + row];
        if diagonal.abs() < 1e-12 {
            return None;
        }
        x[row] = sum / diagonal;
    }
    Some(x)
}

fn invert(matrix: &[f64], n: usize) -> Option<Vec<f64>> {
    let mut a = matrix.to_vec();
    let mut inverse = vec![0.0; n * n];
    for i in 0..n {
        inverse[i * n + i] = 1.0;
    }
    for col in 0..n {
        let pivot = pivot_row(&a, n, col)?;
        if pivot != col {
            swap_rows(&mut a, n, pivot, col);
            swap_rows(&mut inverse, n, pivot, col);
        }
        let diagonal = a[col * n + col];
        if diagonal.abs() < 1e-12 {
            return None;
        }
        for k in 0..n {
            a[col * n + k] /= diagonal;
            inverse[col * n + k] /= diagonal;
        }
        for row in 0..n {
            if row == col {
                continue;
            }
            let factor = a[row * n + col];
            if factor == 0.0 {
                continue;
            }
            for k in 0..n {
                a[row * n + k] -= factor * a[col * n + k];
                inverse[row * n + k] -= factor * inverse[col * n + k];
            }
        }
    }
    Some(inverse)
}

fn pivot_row(a: &[f64], n: usize, col: usize) -> Option<usize> {
    let mut pivot = col;
    for row in (col + 1)..n {
        if a[row * n + col].abs() > a[pivot * n + col].abs() {
            pivot = row;
        }
    }
    if a[pivot * n + col].abs() < 1e-12 {
        None
    } else {
        Some(pivot)
    }
}

fn swap_rows(a: &mut [f64], n: usize, left: usize, right: usize) {
    if left == right {
        return;
    }
    for col in 0..n {
        a.swap(left * n + col, right * n + col);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pair(lo: u16, hi: u16, games: u64, lo_wins: u64) -> PairCounts {
        PairCounts {
            lo,
            hi,
            games: games as f64,
            lo_wins: lo_wins as f64,
            lo_radiant_games: games as f64 / 2.0,
            lo_radiant_wins: lo_wins as f64 / 2.0,
        }
    }

    #[test]
    fn recovers_a_clear_ordering() {
        let pairs = vec![
            pair(1, 2, 100, 73),
            pair(1, 3, 100, 88),
            pair(2, 3, 100, 73),
        ];
        let heroes = vec![
            HeroCounts {
                hero_id: 1,
                games: 200,
                wins: 145,
            },
            HeroCounts {
                hero_id: 2,
                games: 200,
                wins: 135,
            },
            HeroCounts {
                hero_id: 3,
                games: 200,
                wins: 120,
            },
        ];
        let result = fit(&pairs, &heroes, FitOptions::default());
        assert!(result.converged);
        let rating = |id: u16| {
            result
                .ratings
                .iter()
                .find(|r| r.hero_id == id)
                .unwrap()
                .rating
        };
        assert!(rating(1) > rating(2));
        assert!(rating(2) > rating(3));
        assert!(result.ratings.iter().all(|r| r.rating_se.is_finite()));
    }

    #[test]
    fn detects_radiant_advantage() {
        let pairs = vec![PairCounts {
            lo: 1,
            hi: 2,
            games: 100.0,
            lo_wins: 65.0,
            lo_radiant_games: 50.0,
            // Lo wins 70% on radiant and 60% on dire.
            lo_radiant_wins: 35.0,
        }];
        let heroes = vec![
            HeroCounts {
                hero_id: 1,
                games: 100,
                wins: 65,
            },
            HeroCounts {
                hero_id: 2,
                games: 100,
                wins: 35,
            },
        ];
        let result = fit(&pairs, &heroes, FitOptions::default());
        assert!(result.radiant_advantage > 0.0);
        assert!(result.converged);
    }

    #[test]
    fn handles_an_empty_dataset() {
        let result = fit(&[], &[], FitOptions::default());
        assert!(result.ratings.is_empty());
        assert!(result.converged);
    }

    #[test]
    fn solves_and_inverts_matrices() {
        let matrix = vec![4.0, 1.0, 1.0, 3.0];
        let rhs = vec![1.0, 2.0];
        let solution = solve(&matrix, &rhs, 2).unwrap();
        assert!((solution[0] - 1.0 / 11.0).abs() < 1e-9);
        assert!((solution[1] - 7.0 / 11.0).abs() < 1e-9);
        let inverse = invert(&matrix, 2).unwrap();
        assert!((inverse[0] - 3.0 / 11.0).abs() < 1e-9);
        assert!((inverse[1] + 1.0 / 11.0).abs() < 1e-9);
    }
}
