import { useMemo, useState } from 'react';
import {
  createColumnHelper,
  flexRender,
  getCoreRowModel,
  getSortedRowModel,
  useReactTable,
  type ColumnDef,
  type SortingState,
} from '@tanstack/react-table';
import { ArrowDown, ArrowUp, ChevronsUpDown } from 'lucide-react';
import type { RecentAlert, Severity } from '../../types';
import { AlertRowHoverCard } from '../hover/AlertRowHoverCard';
import { useAlertRowHover } from '../../hooks/useAlertRowHover';
import { SkeletonRows } from '../skeleton/Skeleton';

/**
 * Shared headless table for any alert list. TanStack provides sorting,
 * column-definition typing, and a stable rendering loop; we drop the
 * markup straight into the existing `.alerts-table` styles so the look
 * matches what was there before.
 *
 * Variants:
 *   - `compact` (default) — Time / Severity / Storage / IP / Event
 *     (used by RecentCriticalAlertsCard)
 *   - `full`              — adds Vendor + Site columns
 *     (used by AlertDetailsModal)
 */
const SEV_COLOR: Record<Severity, string> = {
  critical:      '#f87171',
  error:         '#f9a8a3',
  warning:       '#fb923c',
  notice:        '#facc15',
  informational: '#60a5fa',
};

const ROW_CLASS: Record<Severity, string> = {
  critical:      'alerts-row--critical',
  error:         'alerts-row--error',
  warning:       'alerts-row--warning',
  notice:        'alerts-row--notice',
  informational: 'alerts-row--informational',
};

const SEVERITY_RANK: Record<Severity, number> = {
  critical: 0, error: 1, warning: 2, notice: 3, informational: 4,
};

const titleCase = (s: string) => s.charAt(0).toUpperCase() + s.slice(1);

interface Props {
  alerts:    RecentAlert[];
  variant?:  'compact' | 'full';
  emptyText?: string;
  /** Render a "Page X of Y" pager below the table. Off for compact. */
  paginate?: boolean;
  pageSize?: number;
  /** Use infinite scroll instead of pagination. */
  infinite?: boolean;
  /** Show loading skeleton rows */
  loading?: boolean;
}

const columnHelper = createColumnHelper<RecentAlert>();

export function AlertsDataTable({
  alerts,
  variant   = 'compact',
  emptyText = 'No alerts',
  paginate  = false,
  pageSize  = 25,
  loading   = false,
}: Props) {
  const { hover, onMove, onLeave } = useAlertRowHover();

  const [sorting, setSorting] = useState<SortingState>([{ id: 'ts', desc: true }]);
  const [page, setPage]       = useState(0);

  const columns = useMemo<ColumnDef<RecentAlert, any>[]>(() => {
    const cols: ColumnDef<RecentAlert, any>[] = [
      columnHelper.accessor('ts', {
        id:     'ts',
        header: 'Time Stamp',
        cell:   ({ row }) => row.original.time,
        sortingFn: (a, b) => a.original.ts - b.original.ts,
        size:   170,
      }),
      columnHelper.accessor('severity', {
        id:     'severity',
        header: 'Severity',
        cell:   ({ getValue }) => {
          const sev = getValue() as Severity;
          return (
            <span className="alert-list__sev" style={{ color: SEV_COLOR[sev] }}>
              {sev}
            </span>
          );
        },
        sortingFn: (a, b) =>
          SEVERITY_RANK[a.original.severity] - SEVERITY_RANK[b.original.severity],
        size:   100,
      }),
    ];

    if (variant === 'full') {
      cols.push(
        columnHelper.accessor('vendor', {
          id:     'vendor',
          header: 'Vendor',
          cell:   ({ getValue }) => titleCase(String(getValue())),
          size:   90,
        }),
        columnHelper.accessor('location', {
          id:     'location',
          header: 'Site',
          cell:   ({ getValue }) => getValue(),
          size:   70,
        }),
      );
    }

    cols.push(
      columnHelper.accessor('storageName', {
        id:        'storageName',
        header:    'Storage Name',
        cell:      ({ getValue }) => (
          <span className="cell-mono">{String(getValue())}</span>
        ),
        size:      220,
      }),
      columnHelper.accessor('ip', {
        id:        'ip',
        header:    'Storage IP',
        cell:      ({ getValue }) => (
          <span className="cell-mono">{String(getValue())}</span>
        ),
        size:      130,
      }),
      columnHelper.accessor('event', {
        id:        'event',
        header:    'Event Details',
        cell:      ({ getValue }) => (
          <span className="cell-truncate" title={String(getValue())}>
            {String(getValue())}
          </span>
        ),
        enableSorting: false,
      }),
    );

    return cols;
  }, [variant]);

  const table = useReactTable({
    data: alerts,
    columns,
    state: { sorting },
    onSortingChange: setSorting,
    getCoreRowModel:   getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getRowId: (row, index) => `${row.ts}-${row.storageName}-${row.event}-${index}`,
  });

  const allRows = table.getRowModel().rows;
  const totalPages = paginate ? Math.max(1, Math.ceil(allRows.length / pageSize)) : 1;
  const clampedPage = Math.min(page, totalPages - 1);
  const rows = paginate
    ? allRows.slice(clampedPage * pageSize, (clampedPage + 1) * pageSize)
    : allRows;

  const colCount = columns.length;

  return (
    <>
      <div className="table-wrap">
        <table className="alerts-table">
          <thead>
            {table.getHeaderGroups().map(hg => (
              <tr key={hg.id}>
                {hg.headers.map(h => {
                  const dir = h.column.getIsSorted();
                  const canSort = h.column.getCanSort();
                  return (
                    <th
                      key={h.id}
                      className={canSort ? 'is-sortable' : ''}
                      onClick={canSort ? h.column.getToggleSortingHandler() : undefined}
                    >
                      <span className="th__label">
                        {flexRender(h.column.columnDef.header, h.getContext())}
                        {canSort && (
                          <span className="th__sort" aria-hidden="true">
                            {dir === 'asc'
                              ? <ArrowUp   size={10} />
                              : dir === 'desc'
                              ? <ArrowDown size={10} />
                              : <ChevronsUpDown size={10} />}
                          </span>
                        )}
                      </span>
                    </th>
                  );
                })}
              </tr>
            ))}
          </thead>
          <tbody>
            {loading ? (
              <SkeletonRows cells={colCount} rows={paginate ? pageSize : 6} />
            ) : rows.length === 0 ? (
              <tr>
                <td colSpan={colCount} className="empty-row">
                  {emptyText}
                </td>
              </tr>
            ) : (
              rows.map(row => {
                const a = row.original;
                return (
                  <tr
                    key={row.id}
                    className={`alerts-row ${ROW_CLASS[a.severity]}`}
                    onMouseEnter={onMove(a)}
                    onMouseMove={onMove(a)}
                    onMouseLeave={onLeave}
                  >
                    {row.getVisibleCells().map(cell => (
                      <td key={cell.id}>
                        {flexRender(cell.column.columnDef.cell, cell.getContext())}
                      </td>
                    ))}
                  </tr>
                );
              })
            )}
          </tbody>
        </table>
      </div>

      {paginate && allRows.length > pageSize && (
        <div className="alerts-pager">
          <button
            type="button"
            className="alerts-pager__btn"
            onClick={() => setPage(p => Math.max(0, p - 1))}
            disabled={clampedPage <= 0}
          >
            Prev
          </button>
          <span className="alerts-pager__info">
            Page {clampedPage + 1} of {totalPages}
            <span className="alerts-pager__count">
              ({allRows.length} alert{allRows.length === 1 ? '' : 's'})
            </span>
          </span>
          <button
            type="button"
            className="alerts-pager__btn"
            onClick={() => setPage(p => Math.min(totalPages - 1, p + 1))}
            disabled={clampedPage >= totalPages - 1}
          >
            Next
          </button>
        </div>
      )}

      {hover && (
        <AlertRowHoverCard alert={hover.alert} x={hover.x} y={hover.y} />
      )}
    </>
  );
}
