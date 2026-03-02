from datetime import datetime
from dataclasses import dataclass

import pyomo.environ as pyo
from pyomo.opt import OptSolver
from pyomo.environ import value


@dataclass
class State():
    timestamp: datetime  # TODO: probably want to pull these out...
    system_lambda: float
    buses_to_lmps: dict[str, float]
    contingencies_to_shadow_prices: dict[str, float]


class LosslessShiftFactorEstimator():
    """Estimate shift factors from a collection of system states by minimizing L2 norm of the shift factors"""

    _model: pyo.Model
    
    def __init__(
        self,
        states: list[State],
        solver: OptSolver
    ):
        model = pyo.ConcreteModel()

        distinct_buses = set().union(*[set(s.buses_to_lmps.keys()) for s in states])
        distinct_contingencies = set().union(*[set(s.contingencies_to_shadow_prices.keys()) for s in states])

        model.BUSES = pyo.Set(initialize=distinct_buses, ordered=False)
        model.CONTINGENCIES = pyo.Set(initialize=distinct_contingencies, ordered=False)

        # create the cartesian product of buses, contingencies
        model.COMBOS = model.BUSES * model.CONTINGENCIES

        # create a shift factor for each cartesian product
        model.shift_factors = pyo.Var(model.COMBOS, domain=pyo.Reals, bounds=(-1,1))  # shift factors bounded between -1,1

        # at each system state
        for state in states:
            # relate congestion component at each bus to shift factors
            def congestion_rule(m, b):
                cong_component = state.buses_to_lmps[b] - state.system_lambda
                sum_of_shadow_prices = sum(m.shift_factors[(b, c)]*state.contingencies_to_shadow_prices.get(c, 0) for c in m.CONTINGENCIES)
                return cong_component == sum_of_shadow_prices
            model.add_component(
                f'cong_{state.timestamp}',
                pyo.Constraint(model.BUSES, rule=congestion_rule)
            )

        # objective to minimze the L2 norm of shift factors
        model.obj = pyo.Objective(
            expr = sum(model.shift_factors[i]**2 for i in model.COMBOS),
            sense=pyo.minimize
        )

        # save model
        self._model = model

        self._solver = solver


    def estimate(self) -> dict[tuple[str, str], float]:
        """ Returns a dictionary mapping (bus, line) pairs to shift factors"""
        self._solver.solve(self._model)
        results_dict = {idx: value(self._model.shift_factors[idx]) for idx in self._model.shift_factors}
        return results_dict


class LossyShiftFactorEstimator():
    """Estimate shift factors from a collection of system states by minimizing L2 norm of shift factor errors in each state"""

    _model: pyo.Model
    
    def __init__(
        self,
        states: list[State],
        solver: OptSolver
    ):
        model = pyo.ConcreteModel()

        distinct_buses = set().union(*[set(s.buses_to_lmps.keys()) for s in states])
        distinct_contingencies = set().union(*[set(s.contingencies_to_shadow_prices.keys()) for s in states])

        model.BUSES = pyo.Set(initialize=distinct_buses, ordered=False)
        model.CONTINGENCIES = pyo.Set(initialize=distinct_contingencies, ordered=False)

        # create the cartesian product of buses, contingencies
        model.SF_INDICES = model.BUSES * model.CONTINGENCIES

        # create a shift factor for each cartesian product
        model.shift_factors = pyo.Var(model.SF_INDICES, domain=pyo.Reals, bounds=(-1,1))  # shift factors bounded between -1,1

        # introduce an error term for each shift factor in each state
        model.STATES = pyo.Set(initialize=[state.timestamp for state in states])
        model.ERROR_INDICES = model.SF_INDICES * model.STATES

        model.errors = pyo.Var(model.ERROR_INDICES, domain=pyo.Reals)  # TODO: can this be bounded?

        # at each system state
        for state in states:
            # relate congestion component at each bus to shift factors
            def congestion_rule(m, b):
                cong_component = state.buses_to_lmps[b] - state.system_lambda
                sum_of_shadow_prices = sum(
                    (m.shift_factors[(b, c)] + m.errors[(b, c, state.timestamp)]) * 
                    state.contingencies_to_shadow_prices.get(c, 0) for c in m.CONTINGENCIES)
                return cong_component == sum_of_shadow_prices
            model.add_component(
                f'cong_{state.timestamp}',
                pyo.Constraint(model.BUSES, rule=congestion_rule)
            )

        # objective to minimze the L2 norm of errors
        model.obj = pyo.Objective(
            expr = sum(model.errors[i]**2 for i in model.ERROR_INDICES),
            sense=pyo.minimize
        )

        # save model
        self._model = model

        self._solver = solver


    def estimate(self) -> dict[tuple[str, str], float]:
        """ Returns a dictionary mapping (bus, line) pairs to shift factors"""
        self._solver.solve(self._model)
        results_dict = {idx: value(self._model.shift_factors[idx]) for idx in self._model.shift_factors}
        return results_dict

    def get_errors(self):
        results_dict = {idx: value(self._model.errors[idx]) for idx in self._model.errors}
        return results_dict
