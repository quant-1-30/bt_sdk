#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>
#include <pybind11/stl.h>
#include "factor.hpp"

namespace py = pybind11;
using namespace rpc_feed::adjust;  // 避免写长命名空间


PYBIND11_MODULE(adj_factor, m) {
    py::class_<AdjustmentEvent>(m, "AdjustmentEvent")
        .def(py::init<>())
        .def_readwrite("ex_date", &AdjustmentEvent::ex_date)
        .def_readwrite("bonus_share", &AdjustmentEvent::bonus_share)
        .def_readwrite("transfer", &AdjustmentEvent::transfer)
        .def_readwrite("bonus", &AdjustmentEvent::bonus);

    py::class_<RightmentEvent>(m, "RightmentEvent")
        .def(py::init<>())
        .def_readwrite("ex_date", &RightmentEvent::ex_date)
        .def_readwrite("price", &RightmentEvent::price)
        .def_readwrite("ratio", &RightmentEvent::ratio);

    py::enum_<AdjustType>(m, "AdjustType")
        .value("Forward", AdjustType::Forward)
        .value("Backward", AdjustType::Backward);

    py::class_<rpc_feed::adjust::FactorResult>(m, "FactorResult")
        .def_readonly("raw_factors", &rpc_feed::adjust::FactorResult::raw_factors)
        .def_readonly("adj_factors", &rpc_feed::adjust::FactorResult::adj_factors);

    // Accept contiguous NumPy arrays directly (zero-copy from Polars .to_numpy())
    // while still coercing Python sequences via py::array::forcecast for callers/tests
    // that pass plain lists. Extracting raw pointers before gil_scoped_release avoids
    // holding the GIL to copy thousands of elements into std::vector<int/double>.
    m.def("calc_adjust_factors",
          [](py::array_t<int, py::array::c_style | py::array::forcecast> trading_dates,
             py::array_t<double, py::array::c_style | py::array::forcecast> close,
             const std::vector<AdjustmentEvent>& adj_events,
             const std::vector<RightmentEvent>& right_events,
             AdjustType type) {
              const int* dates_ptr = trading_dates.data();
              const size_t n_dates = static_cast<size_t>(trading_dates.size());
              const double* close_ptr = close.data();
              const size_t n_close = static_cast<size_t>(close.size());
              py::gil_scoped_release release;
              return calc_adjust_factors(
                  dates_ptr, n_dates, close_ptr, n_close, adj_events, right_events, type);
          },
          py::arg("trading_dates"),
          py::arg("close"),
          py::arg("adj_events"),
          py::arg("right_events"),
          py::arg("type"));

    // Fully columnar zero-copy entry point: takes raw NumPy column views for
    // trading_dates/close AND adjustment/rightment columns directly, eliminating
    // Python-side AdjustmentEvent/RightmentEvent loop allocation.
    m.def("calc_adjust_factors_columns",
          [](py::array_t<int, py::array::c_style | py::array::forcecast> trading_dates,
             py::array_t<double, py::array::c_style | py::array::forcecast> close,
             py::array_t<int, py::array::c_style | py::array::forcecast> adj_ex_dates,
             py::array_t<double, py::array::c_style | py::array::forcecast> adj_bonus_share,
             py::array_t<double, py::array::c_style | py::array::forcecast> adj_transfer,
             py::array_t<double, py::array::c_style | py::array::forcecast> adj_bonus,
             py::array_t<int, py::array::c_style | py::array::forcecast> rgt_ex_dates,
             py::array_t<double, py::array::c_style | py::array::forcecast> rgt_price,
             py::array_t<double, py::array::c_style | py::array::forcecast> rgt_ratio,
             AdjustType type) {
              const size_t n_adj = static_cast<size_t>(adj_ex_dates.size());
              if (static_cast<size_t>(adj_bonus_share.size()) != n_adj ||
                  static_cast<size_t>(adj_transfer.size()) != n_adj ||
                  static_cast<size_t>(adj_bonus.size()) != n_adj) {
                  throw std::invalid_argument("adjustment column sizes must match");
              }
              const size_t n_rgt = static_cast<size_t>(rgt_ex_dates.size());
              if (static_cast<size_t>(rgt_price.size()) != n_rgt ||
                  static_cast<size_t>(rgt_ratio.size()) != n_rgt) {
                  throw std::invalid_argument("rightment column sizes must match");
              }
              const int* dates_ptr = trading_dates.data();
              const size_t n_dates = static_cast<size_t>(trading_dates.size());
              const double* close_ptr = close.data();
              const size_t n_close = static_cast<size_t>(close.size());
              const int* adj_dates_ptr = adj_ex_dates.data();
              const double* adj_bs_ptr = adj_bonus_share.data();
              const double* adj_tr_ptr = adj_transfer.data();
              const double* adj_bn_ptr = adj_bonus.data();
              const int* rgt_dates_ptr = rgt_ex_dates.data();
              const double* rgt_pr_ptr = rgt_price.data();
              const double* rgt_rt_ptr = rgt_ratio.data();
              py::gil_scoped_release release;
              return calc_adjust_factors_columns(
                  dates_ptr, n_dates, close_ptr, n_close,
                  adj_dates_ptr, adj_bs_ptr, adj_tr_ptr, adj_bn_ptr, n_adj,
                  rgt_dates_ptr, rgt_pr_ptr, rgt_rt_ptr, n_rgt,
                  type);
          },
          py::arg("trading_dates"),
          py::arg("close"),
          py::arg("adj_ex_dates"),
          py::arg("adj_bonus_share"),
          py::arg("adj_transfer"),
          py::arg("adj_bonus"),
          py::arg("rgt_ex_dates"),
          py::arg("rgt_price"),
          py::arg("rgt_ratio"),
          py::arg("type"));
}