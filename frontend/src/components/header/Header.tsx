import type { Location, SystemStatus, TimeRange } from '../../types';
import { BrandTitle } from './BrandTitle';
import { LiveBadge, type ServiceStatusRow } from './LiveBadge';
import { LocationPicker } from './LocationPicker';
import { RangePicker } from './RangePicker';
import { RefreshButton } from './RefreshButton';

interface Props {
  status: SystemStatus;
  selectedLocations: Location[];
  allLocations: Location[];
  onLocationsChange: (next: Location[]) => void;
  range: TimeRange;
  onRangeChange: (next: TimeRange) => void;
  refreshing: boolean;
  onRefresh: () => void;
  services?: ServiceStatusRow[];
}

export function Header({
  status,
  selectedLocations,
  allLocations,
  onLocationsChange,
  range,
  onRangeChange,
  refreshing,
  onRefresh,
  services,
}: Props) {
  return (
    <header className="header">
      <div className="header__left">
      </div>

      <div className="header__center">
        <BrandTitle />
      </div>

      <div className="header__actions">
        <LiveBadge status={status} services={services} />
        <LocationPicker
          selected={selectedLocations}
          all={allLocations}
          onChange={onLocationsChange}
        />
        <RefreshButton busy={refreshing} onClick={onRefresh} />
        <RangePicker value={range} onChange={onRangeChange} />
      </div>
    </header>
  );
}
