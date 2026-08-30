#include <pybind11/pybind11.h>
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

    m.def("calc_adjust_factors", &calc_adjust_factors,
          py::arg("trading_dates"),
          py::arg("close"),
          py::arg("adj_events"),
          py::arg("right_events"),
          py::arg("type"),
          py::call_guard<py::gil_scoped_release>()); // key note: release GIL for long-running C++ function
}