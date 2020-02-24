exports.pipeline = function (state, fns, final) {
  let idx = -1
  next(state)
  function next (state) {
    idx = idx + 1
    if (state === null) return final(state)
    if (idx === fns.length) return final(state)
    if (!fns[idx]) return next(state)
    fns[idx](state, next)
  }
}
