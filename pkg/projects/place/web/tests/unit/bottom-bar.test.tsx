// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { fireEvent, render, screen } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'
import { BottomBar } from '@/components/bottom-bar'

function bar(props: { layerVisible?: boolean; onLayerVisible?: (visible: boolean) => void } = {}) {
  return render(
    <BottomBar
      color={1}
      onColor={vi.fn()}
      hover={null}
      count={12}
      layerVisible={props.layerVisible ?? true}
      onLayerVisible={props.onLayerVisible ?? vi.fn()}
      committing={false}
      onCommit={vi.fn()}
      onClear={vi.fn()}
      notice={null}
      message={null}
    />,
  )
}

describe('BottomBar layer', () => {
  it('names the pending pixels as your layer and the commit as a merge to the canvas', () => {
    // Database words like commit hide that the pixels are private until merged and public after.
    bar()

    expect(screen.getByRole('button', { name: 'Merge 12 px to canvas' })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Delete layer' })).toBeInTheDocument()
    expect(screen.getByText('12 / 4096 px')).toBeInTheDocument()
    expect(screen.getByText('Your layer')).toBeInTheDocument()
  })

  it('toggles the layer visibility from the eye button', () => {
    // The eye must flip the state it shows, or hiding the layer to compare with the canvas is impossible.
    const onLayerVisible = vi.fn()
    const { rerender } = bar({ layerVisible: true, onLayerVisible })

    fireEvent.click(screen.getByRole('button', { name: 'Hide your layer' }))
    expect(onLayerVisible).toHaveBeenLastCalledWith(false)

    rerender(
      <BottomBar
        color={1}
        onColor={vi.fn()}
        hover={null}
        count={12}
        layerVisible={false}
        onLayerVisible={onLayerVisible}
        committing={false}
        onCommit={vi.fn()}
        onClear={vi.fn()}
        notice={null}
        message={null}
      />,
    )
    fireEvent.click(screen.getByRole('button', { name: 'Show your layer' }))
    expect(onLayerVisible).toHaveBeenLastCalledWith(true)
  })

  it('keeps merge and delete usable while the layer is hidden', () => {
    // Hiding is only a view; blocking the merge would strand the user's pixels behind a closed eye.
    bar({ layerVisible: false })

    expect(screen.getByRole('button', { name: 'Merge 12 px to canvas' })).toBeEnabled()
    expect(screen.getByRole('button', { name: 'Delete layer' })).toBeEnabled()
  })
})
